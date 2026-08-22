"""
Bronze layer: lands raw source extracts into DuckDB, tagged with domain_key,
source_system, source_record_id (natural key), and load metadata (_load_ts,
_load_batch_id).

Phase 2 change (generic attribute-bag): bronze is now ONE shared table,
bronze.raw_records, used by every domain and every source -- not a dedicated
table per source (crm_customers_raw / erp_customers_raw are gone). Whatever
columns a source's extract has are packed verbatim into a JSON `attributes`
column; this file contains no Customer/CRM/ERP-specific SQL or field lists,
only ordinary source-connector config (SOURCES below: which raw column is
the natural key, which is the watermark date) -- the same kind of config any
new domain's source would need, never a branch on domain or source identity.
Field mapping/standardization (e.g. ERP's "Last, First" full_name split, or
which raw column means "email" for a given source) happens downstream, in
dbt_project/models/silver/stg_source_records.sql, driven by
governance.source_field_mappings -- NOT here. Bronze stores exactly what the
source sent, untouched, just reshaped into JSON.

Append-only for INCREMENTAL loads: every incremental run INSERTs new rows
rather than overwriting -- bronze preserves full load history for a source
once it's past its first load. "Current state" for a given source_record_id
is whichever row has the most recent _load_ts; that collapse happens
downstream in the silver staging model, not here. FULL loads (see below) are
the one case that removes rows, and only that source's own rows.

Load-type detection: bronze.load_control (domain_key, source_system,
load_type, watermark_date, last_loaded_at, row_count) tracks each
domain+source's load history.
  - No control row for a domain+source -> FULL load (reads the whole extract).
  - A control row already exists -> INCREMENTAL load, filtering the extract to
    rows whose watermark column >= the stored watermark_date.
    (>= not > -- a record updated again on the same calendar day as the
    watermark must never be silently dropped; re-landing an unchanged row on
    the boundary day is harmless since staging always picks the latest row
    per key by _load_ts.)
  - --force-full overrides detection entirely: does a full load regardless of
    history, replacing bronze for every configured source and resetting
    their control rows. Does NOT need to separately touch silver/gold --
    those are `table`-materialized dbt models that already fully rebuild
    from bronze on every `dbt run`/`build_pipeline.py`, incremental or not.
  - Because bronze.raw_records is now a table SHARED across every
    domain/source (unlike the old per-source dedicated tables), a FULL load
    DELETEs only that (domain_key, source_system)'s own rows before
    re-inserting -- it must never touch another source's or domain's rows.

Logical deletes: source extracts may include an is_deleted column (see
data/generate_source_data.py). Bronze extracts it into its own structural
`is_deleted` column (config-driven per SOURCES entry, same as
natural_key_col/modified_date_col -- not a domain business attribute, so it
does not go through governance.domain_attributes/source_field_mappings) --
it also stays inside `attributes` like every other raw field, untouched.
Bronze lands it as-is; silver is where it actually gets filtered out.

bronze.load_batches: an append-only log, one row per invocation of this
script (never updated -- same convention as audit.audit_trail and
stewardship.match_review_log), recording the load_batch_id (the same UUID
every row landed by this run carries, across all domains/sources, full or
incremental), the overall load_type ('FULL' if ANY source this run did a
full load, else 'INCREMENTAL' -- a mixed run is treated as FULL since that's
the safe direction), which "domain_key.source_system" combos it touched, and
the total row count landed. This is separate from bronze.load_control (which
stays per domain+source, for watermark filtering) because load_type/batch
identity is a property of the RUN, not of any one source.
dbt_project/macros looks this table up at compile time to know which bronze
rows belong to "the current batch" for the incremental silver staging model;
this project's build_pipeline.py reads its load_type to decide whether to
pass --full-refresh to the silver dbt run.

Row volumes here are demo-scale (hundreds of rows per source), so per-row
inserts via executemany are simple and fast enough; a production connector
landing much larger extracts would want a bulk JSON load instead.

Product domain onboarding added xlsx support: SUPPLIER's extract is landed as
a spreadsheet (data/generate_product_source_data.py), not a CSV, mirroring a
real supplier portal's own export format being different from an internal
system's CSV feed. Each SOURCES entry now carries a file_type ('csv', the
default, or 'xlsx'); an 'xlsx' entry is read with pandas.read_excel(...,
dtype=str) -- dtype=str mirrors read_csv_auto's ALL_VARCHAR=TRUE for CSV, so
every column arrives as a raw string regardless of file format, with no type
inference of its own to disagree with -- then registered as an ordinary
DuckDB view via con.register() so every line of SQL below (watermark
filtering, column introspection, row extraction) runs identically
regardless of which file_type a source uses. No per-file-type branch exists
anywhere else in this file.

Usage:
    python scripts/load_bronze.py                # auto: full first time, incremental after
    python scripts/load_bronze.py --force-full    # explicit full rebuild, ignores load history
"""
import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
DB_PATH = str(BASE / "mdm_demo.duckdb")
# forward slashes for SQL string literals -- avoids backslash-escaping pitfalls on Windows
CRM_CSV = (BASE / "data" / "sources" / "crm_customers.csv").as_posix()
ERP_CSV = (BASE / "data" / "sources" / "erp_customers.csv").as_posix()
ERP_PRODUCTS_CSV = (BASE / "data" / "sources" / "erp_products.csv").as_posix()
PLM_PRODUCTS_CSV = (BASE / "data" / "sources" / "plm_products.csv").as_posix()
SUPPLIER_PRODUCTS_XLSX = (BASE / "data" / "sources" / "supplier_products.xlsx").as_posix()

# Source-connector config only (which raw column is the natural key / the
# watermark date / the logical-delete flag / the file format) -- not domain
# business logic. A new domain's source is onboarded by adding an entry
# here, never by adding code. is_deleted_col is extracted into its own
# structural bronze column (like source_record_id) rather than left for a
# dbt model to dig out of `attributes` by assuming every source names it
# literally "is_deleted" -- whether/how a source marks logical deletes is a
# connector-level technical concern, not domain business data, so it's
# config here, not a governed domain_attributes/source_field_mappings row.
# file_type: 'csv' (default, read via read_csv_auto) | 'xlsx' (read via
# pandas.read_excel, see module docstring).
SOURCES = [
    {
        "domain_key": "customer",
        "source_system": "CRM",
        "file_type": "csv",
        "file_path": CRM_CSV,
        "natural_key_col": "customer_id",
        "modified_date_col": "modified_date",
        "is_deleted_col": "is_deleted",
    },
    {
        "domain_key": "customer",
        "source_system": "ERP",
        "file_type": "csv",
        "file_path": ERP_CSV,
        "natural_key_col": "customer_number",
        "modified_date_col": "last_updated",
        "is_deleted_col": "is_deleted",
    },
    {
        "domain_key": "product",
        "source_system": "ERP",
        "file_type": "csv",
        "file_path": ERP_PRODUCTS_CSV,
        "natural_key_col": "item_number",
        "modified_date_col": "last_modified",
        "is_deleted_col": "is_deleted",
    },
    {
        "domain_key": "product",
        "source_system": "PLM",
        "file_type": "csv",
        "file_path": PLM_PRODUCTS_CSV,
        "natural_key_col": "part_id",
        "modified_date_col": "date_modified",
        "is_deleted_col": "is_deleted",
    },
    {
        "domain_key": "product",
        "source_system": "SUPPLIER",
        "file_type": "xlsx",
        "file_path": SUPPLIER_PRODUCTS_XLSX,
        "natural_key_col": "supplier_sku",
        "modified_date_col": "date_updated",
        "is_deleted_col": "is_deleted",
    },
]


def _ensure_bronze_tables(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.raw_records (
            domain_key VARCHAR,
            source_system VARCHAR,
            source_record_id VARCHAR,
            is_deleted BOOLEAN,       -- structural logical-delete flag, see SOURCES.is_deleted_col
            attributes JSON,          -- every raw column the source sent, verbatim
            _load_ts TIMESTAMP,
            _load_batch_id VARCHAR
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.load_control (
            domain_key VARCHAR,
            source_system VARCHAR,
            load_type VARCHAR,       -- 'FULL' | 'INCREMENTAL'
            watermark_date DATE,     -- max watermark value as of last successful load
            last_loaded_at TIMESTAMP,
            row_count INTEGER,       -- rows loaded/appended by the most recent run
            PRIMARY KEY (domain_key, source_system)
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.load_batches (
            load_batch_id VARCHAR PRIMARY KEY,  -- the UUID shared by every row this run landed, all sources
            load_type VARCHAR,                  -- 'FULL' | 'INCREMENTAL' for the run as a whole
            sources VARCHAR[],                   -- 'domain_key.source_system' combos touched by this run
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
    sources = [f"{r['domain_key']}.{r['source_system']}" for r in source_results]
    total_rows = sum(r["row_count"] for r in source_results)
    con.execute("""
        INSERT INTO bronze.load_batches (load_batch_id, load_type, sources, started_at, row_count)
        VALUES (?, ?, ?, ?, ?)
    """, [batch_id, overall_type, sources, started_at, total_rows])


def _get_control_row(con, domain_key, source_system):
    row = con.execute(
        "SELECT load_type, watermark_date, last_loaded_at, row_count "
        "FROM bronze.load_control WHERE domain_key = ? AND source_system = ?",
        [domain_key, source_system],
    ).fetchone()
    if row is None:
        return None
    return {"load_type": row[0], "watermark_date": row[1], "last_loaded_at": row[2], "row_count": row[3]}


def _upsert_control_row(con, domain_key, source_system, load_type, watermark_date, row_count):
    con.execute("""
        INSERT INTO bronze.load_control (domain_key, source_system, load_type, watermark_date, last_loaded_at, row_count)
        VALUES (?, ?, ?, ?, current_timestamp, ?)
        ON CONFLICT (domain_key, source_system) DO UPDATE SET
            load_type = excluded.load_type,
            watermark_date = excluded.watermark_date,
            last_loaded_at = excluded.last_loaded_at,
            row_count = excluded.row_count;
    """, [domain_key, source_system, load_type, watermark_date, row_count])


def _load_source(con, source, force_full: bool, batch_id: str):
    domain_key = source["domain_key"]
    source_system = source["source_system"]
    natural_key_col = source["natural_key_col"]
    modified_date_col = source["modified_date_col"]
    is_deleted_col = source["is_deleted_col"]
    file_type = source.get("file_type", "csv")

    control = _get_control_row(con, domain_key, source_system)
    mode = "FULL" if (force_full or control is None) else "INCREMENTAL"

    # The only source-specific SQL here is naming WHICH raw column is the
    # watermark (config-driven per SOURCES entry) -- not a branch on domain
    # or source identity. try_cast so a malformed date never aborts the load,
    # it just can't participate in incremental filtering/watermarking.
    if file_type == "xlsx":
        # dtype=str mirrors read_csv_auto's ALL_VARCHAR=TRUE below -- every
        # column arrives as a raw string, matching what a spreadsheet export
        # actually sends, with no pandas type inference of its own to
        # disagree with downstream. con.register() makes the DataFrame
        # queryable as an ordinary view, so every other line in this
        # function (watermark filter, column introspection, row extraction)
        # is identical regardless of file_type.
        df = pd.read_excel(source["file_path"], dtype=str)
        view_name = f"_bronze_src_{domain_key}_{source_system}".lower()
        con.register(view_name, df)
        raw_sql = f"""
            SELECT *, try_cast({modified_date_col} AS DATE) AS _wm_date
            FROM {view_name}
        """
    else:
        raw_sql = f"""
            SELECT *, try_cast({modified_date_col} AS DATE) AS _wm_date
            FROM read_csv_auto('{source['file_path']}', ALL_VARCHAR=TRUE)
        """
    if mode == "INCREMENTAL":
        watermark_date = control["watermark_date"]
        if watermark_date is None:
            # Defensive fallback (e.g. a prior full load whose extract had no
            # parseable dates at all) -- treat as "include everything" rather
            # than silently loading nothing.
            watermark_date = "1900-01-01"
        raw_sql = f"SELECT * FROM ({raw_sql}) WHERE _wm_date >= '{watermark_date}'::date"

    rel = con.execute(raw_sql)
    cols = [d[0] for d in rel.description]
    rows = rel.fetchall()
    nk_idx = cols.index(natural_key_col)
    wm_idx = cols.index("_wm_date")
    del_idx = cols.index(is_deleted_col)
    attr_cols = [c for c in cols if c != "_wm_date"]
    attr_idx = [cols.index(c) for c in attr_cols]

    if mode == "FULL":
        # bronze.raw_records is shared across every domain/source -- a full
        # reload must only remove THIS (domain_key, source_system)'s rows.
        con.execute(
            "DELETE FROM bronze.raw_records WHERE domain_key = ? AND source_system = ?",
            [domain_key, source_system],
        )

    load_ts = datetime.now(timezone.utc)
    insert_rows = []
    max_wm = None
    for row in rows:
        attributes = {cols[i]: row[i] for i in attr_idx}
        is_deleted = str(row[del_idx]).strip().lower() == "true"
        insert_rows.append(
            (domain_key, source_system, row[nk_idx], is_deleted, json.dumps(attributes), load_ts, batch_id)
        )
        wm_val = row[wm_idx]
        if wm_val is not None and (max_wm is None or wm_val > max_wm):
            max_wm = wm_val

    if insert_rows:
        con.executemany("""
            INSERT INTO bronze.raw_records
                (domain_key, source_system, source_record_id, is_deleted, attributes, _load_ts, _load_batch_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, insert_rows)

    loaded = len(insert_rows)
    if mode == "FULL":
        watermark = max_wm
    else:
        old_watermark = control["watermark_date"]
        watermark = max_wm if (max_wm is not None and (old_watermark is None or str(max_wm) > str(old_watermark))) else old_watermark

    _upsert_control_row(con, domain_key, source_system, mode, watermark, loaded)
    label = "FULL load" if mode == "FULL" else "INCREMENTAL load"
    print(f"  {domain_key}.{source_system}: {label} -- {loaded} rows, watermark={watermark}")
    return {"domain_key": domain_key, "source_system": source_system, "load_type": mode, "row_count": loaded}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force-full", action="store_true",
        help="Full reload regardless of load history (replaces bronze for every configured source, resets control rows).",
    )
    args = parser.parse_args()

    con = duckdb.connect(DB_PATH)
    _ensure_bronze_tables(con)
    batch_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    print(f"Loading bronze (batch {batch_id[:8]}, {'FORCE-FULL' if args.force_full else 'auto-detect'})...")
    results = [_load_source(con, source, args.force_full, batch_id) for source in SOURCES]
    _log_load_batch(con, batch_id, started_at, results)

    con.close()


if __name__ == "__main__":
    main()
