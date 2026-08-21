"""
Bronze layer: lands raw source extracts into DuckDB, tagged with source_system,
source_record_id (natural key), and load metadata (_load_ts, _load_batch_id).

Append-only: every load (full or incremental) INSERTs new rows rather than
overwriting -- bronze preserves full load history. "Current state" for a given
source_record_id is whichever row has the most recent _load_ts; that collapse
happens downstream in the silver staging models (stg_crm_customers.sql /
stg_erp_customers.sql), not here. This keeps bronze's own job simple (land
exactly what was received, untouched) and matches this project's existing
"metadata/state-driven, not hardcoded" convention.

Load-type detection: bronze.load_control (source_system, load_type,
watermark_date, last_loaded_at, row_count) tracks each source's load history.
  - No control row for a source  -> FULL load (reads the whole extract).
  - A control row already exists -> INCREMENTAL load, filtering the extract to
    rows whose modified_date/last_updated >= the stored watermark_date.
    (>= not > -- a record updated again on the same calendar day as the
    watermark must never be silently dropped; re-landing an unchanged row on
    the boundary day is harmless since staging always picks the latest row
    per key by _load_ts.)
  - --force-full overrides detection entirely: does a full load regardless of
    history, replacing bronze for both sources and resetting their control
    rows. Does NOT need to separately touch silver/gold -- those are `table`-
    materialized dbt models that already fully rebuild from bronze on every
    `dbt run`/`build_pipeline.py`, incremental or not.

Logical deletes: source extracts may include an is_deleted column (see
data/generate_source_data.py). Bronze lands it as-is, untouched -- silver is
where it actually gets filtered out (dbt_project/models/silver/silver_all_staged.sql).

bronze.load_batches: an append-only log, one row per invocation of this
script (never updated -- same convention as audit.audit_trail and
stewardship.match_review_log), recording the load_batch_id (the same UUID
every row landed by this run carries, across all sources, full or
incremental -- see _load_ts/_load_batch_id above), the overall load_type
('FULL' if ANY source this run did a full load, else 'INCREMENTAL' -- a
mixed run is treated as FULL since that's the safe direction, though with
today's two sources always loading in lockstep this never actually mixes),
which sources it touched, and the total row count landed. This is separate
from bronze.load_control (which stays per-source, for watermark filtering)
because load_type/batch identity is a property of the RUN, not of any one
source. dbt_project/macros looks this table up at compile time to know
which bronze rows belong to "the current batch" for the incremental silver
staging models (stg_crm_customers.sql / stg_erp_customers.sql); this
project's build_pipeline.py reads its load_type to decide whether to pass
--full-refresh to the silver dbt run.

Usage:
    python scripts/load_bronze.py                # auto: full first time, incremental after
    python scripts/load_bronze.py --force-full    # explicit full rebuild, ignores load history
"""
import argparse
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb

BASE = Path(__file__).resolve().parent.parent
DB_PATH = str(BASE / "mdm_demo.duckdb")
# forward slashes for SQL string literals -- avoids backslash-escaping pitfalls on Windows
CRM_CSV = (BASE / "data" / "sources" / "crm_customers.csv").as_posix()
ERP_CSV = (BASE / "data" / "sources" / "erp_customers.csv").as_posix()

SOURCES = [
    {
        "source_system": "CRM",
        "csv_path": CRM_CSV,
        "table": "crm_customers_raw",
        "natural_key_col": "customer_id",
        "modified_date_col": "modified_date",
    },
    {
        "source_system": "ERP",
        "csv_path": ERP_CSV,
        "table": "erp_customers_raw",
        "natural_key_col": "customer_number",
        "modified_date_col": "last_updated",
    },
]


def _ensure_control_table(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.load_control (
            source_system VARCHAR PRIMARY KEY,
            load_type VARCHAR,       -- 'FULL' | 'INCREMENTAL'
            watermark_date DATE,     -- max source modified_date/last_updated as of last successful load
            last_loaded_at TIMESTAMP,
            row_count INTEGER        -- rows loaded/appended by the most recent run
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.load_batches (
            load_batch_id VARCHAR PRIMARY KEY,  -- the UUID shared by every row this run landed, all sources
            load_type VARCHAR,                  -- 'FULL' | 'INCREMENTAL' for the run as a whole
            sources VARCHAR[],                   -- source_systems touched by this run, e.g. ['CRM', 'ERP']
            started_at TIMESTAMP,
            row_count INTEGER                    -- total rows landed across all sources this run
        );
    """)


def _log_load_batch(con, batch_id: str, started_at, source_results: list):
    """Appends one row to bronze.load_batches for this run -- never updated,
    same append-only convention as audit.audit_trail / match_review_log.
    A run is logged as 'FULL' if ANY source in it did a full load (the safe
    direction: a mixed run being treated as full just costs an extra,
    harmless full-refresh downstream, never a missed delta)."""
    overall_type = "FULL" if any(r["load_type"] == "FULL" for r in source_results) else "INCREMENTAL"
    sources = [r["source_system"] for r in source_results]
    total_rows = sum(r["row_count"] for r in source_results)
    con.execute("""
        INSERT INTO bronze.load_batches (load_batch_id, load_type, sources, started_at, row_count)
        VALUES (?, ?, ?, ?, ?)
    """, [batch_id, overall_type, sources, started_at, total_rows])


def _get_control_row(con, source_system):
    row = con.execute(
        "SELECT load_type, watermark_date, last_loaded_at, row_count "
        "FROM bronze.load_control WHERE source_system = ?",
        [source_system],
    ).fetchone()
    if row is None:
        return None
    return {"load_type": row[0], "watermark_date": row[1], "last_loaded_at": row[2], "row_count": row[3]}


def _upsert_control_row(con, source_system, load_type, watermark_date, row_count):
    con.execute("""
        INSERT INTO bronze.load_control (source_system, load_type, watermark_date, last_loaded_at, row_count)
        VALUES (?, ?, ?, current_timestamp, ?)
        ON CONFLICT (source_system) DO UPDATE SET
            load_type = excluded.load_type,
            watermark_date = excluded.watermark_date,
            last_loaded_at = excluded.last_loaded_at,
            row_count = excluded.row_count;
    """, [source_system, load_type, watermark_date, row_count])


def _load_source(con, source, force_full: bool, batch_id: str):
    source_system = source["source_system"]
    table = source["table"]
    natural_key_col = source["natural_key_col"]
    modified_date_col = source["modified_date_col"]

    control = _get_control_row(con, source_system)
    mode = "FULL" if (force_full or control is None) else "INCREMENTAL"

    base_select = f"""
        SELECT
            '{source_system}' AS source_system,
            {natural_key_col} AS source_record_id,
            current_timestamp AS _load_ts,
            '{batch_id}' AS _load_batch_id,
            *
        FROM read_csv_auto('{source['csv_path']}', ALL_VARCHAR=TRUE)
    """

    if mode == "FULL":
        con.execute(f"CREATE OR REPLACE TABLE bronze.{table} AS {base_select};")
        loaded = con.execute(f"SELECT COUNT(*) FROM bronze.{table}").fetchone()[0]
        watermark = con.execute(
            f"SELECT MAX(try_cast({modified_date_col} AS DATE)) FROM bronze.{table}"
        ).fetchone()[0]
        _upsert_control_row(con, source_system, "FULL", watermark, loaded)
        print(f"  {source_system}: FULL load -- {loaded} rows, watermark={watermark}")
        return {"source_system": source_system, "load_type": "FULL", "row_count": loaded}

    # INCREMENTAL
    watermark_date = control["watermark_date"]
    if watermark_date is None:
        # Defensive fallback (e.g. a prior full load whose extract had no
        # parseable dates at all) -- treat as "include everything" rather
        # than silently loading nothing.
        watermark_date = "1900-01-01"

    filtered_select = base_select + f"""
        WHERE try_cast({modified_date_col} AS DATE) >= '{watermark_date}'::date
    """
    before = con.execute(f"SELECT COUNT(*) FROM bronze.{table}").fetchone()[0]
    con.execute(f"INSERT INTO bronze.{table} {filtered_select};")
    after = con.execute(f"SELECT COUNT(*) FROM bronze.{table}").fetchone()[0]
    appended = after - before

    new_max = con.execute(
        f"""SELECT MAX(try_cast({modified_date_col} AS DATE))
            FROM bronze.{table}
            WHERE try_cast({modified_date_col} AS DATE) >= '{watermark_date}'::date"""
    ).fetchone()[0]
    new_watermark = new_max if (new_max is not None and str(new_max) > str(watermark_date)) else watermark_date
    _upsert_control_row(con, source_system, "INCREMENTAL", new_watermark, appended)
    print(f"  {source_system}: INCREMENTAL load -- {appended} rows appended "
          f"(watermark {watermark_date} -> {new_watermark})")
    return {"source_system": source_system, "load_type": "INCREMENTAL", "row_count": appended}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force-full", action="store_true",
        help="Full reload regardless of load history (replaces bronze for both sources, resets control rows).",
    )
    args = parser.parse_args()

    con = duckdb.connect(DB_PATH)
    _ensure_control_table(con)
    batch_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    print(f"Loading bronze (batch {batch_id[:8]}, {'FORCE-FULL' if args.force_full else 'auto-detect'})...")
    results = [_load_source(con, source, args.force_full, batch_id) for source in SOURCES]
    _log_load_batch(con, batch_id, started_at, results)

    con.close()


if __name__ == "__main__":
    main()
