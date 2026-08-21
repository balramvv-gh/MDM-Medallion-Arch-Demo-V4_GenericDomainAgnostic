"""
Runs the full pipeline build in order, cross-platform (Windows/macOS/Linux):

  0. python scripts/ensure_governed_schemas.py  (provisions ref.*/bus_rules.* --
                                                   see below for why this runs first)
  0.5. python scripts/ensure_domain_metadata.py  (provisions
                                              governance.domain_registry/domain_attributes/
                                              domain_source_systems/domain_relationships/
                                              user_domain_access -- see that script's
                                              docstring; additive-only, order relative to
                                              step 0 doesn't matter, kept adjacent)
  1. dbt seed
  2. dbt run --exclude gold.*          (bronze is already loaded separately, see below)
  2.5. python scripts/cancel_stale_match_reviews.py  (cancels any open Match Review
                                              pair whose source data just changed --
                                              see below, and that script's docstring)
  3. python scripts/generate_matches.py  (embedding-based match/merge -- needs silver)
  4. dbt run --select gold.*           (gold layer consumes the matches from step 3)
  5. python scripts/audit_pipeline_diff.py  (logs creates/updates/logical deletes
                                              to the gold-layer audit trail)

This exists because the gold layer now depends on a Python step (embedding
similarity via scikit-learn) that has to run strictly between silver and gold
-- a single `dbt run` can no longer build the whole pipeline in one command.
See scripts/generate_matches.py's docstring for why.

Step 0 exists because Reference Data Maintenance and Rules Configuration
(Data Governance nav) moved country/state codes and column/matching/
survivorship rules out of dbt seeds into DB-native tables (schemas 'ref' and
'bus_rules', maintained via those screens' maker-checker workflows). Step 2's
silver models now read ref.ref_state_codes/ref_country_codes as a
{{ source(...) }}, and step 4's gold_crosswalk.sql reads
bus_rules.matching_thresholds the same way -- both schemas must already exist
and be populated before dbt runs, which can be before the FastAPI app has
ever started (e.g. a fresh clone). See scripts/ensure_governed_schemas.py's
docstring for the full rationale.

Step 2.5 must run strictly between step 2 (silver just rebuilt) and step 3
(matching runs against that silver) -- it's what implements the "an updated
source+SourceID cancels the stale Match Review pair it was on, before
matching runs again" rule (see scripts/cancel_stale_match_reviews.py and
scripts/generate_matches.py's "Match Review hold" docstring section). Running
it any later would let step 3 hold a record whose review should already have
been cancelled.

Step 5 must run immediately after step 4, before any other write touches
main_gold.gold_customers -- it diffs the freshly rebuilt table against the
audit trail's snapshot of the previous build. If you ever rebuild just the
gold layer manually (`dbt run --select gold.*`) instead of via this script,
run `python scripts/audit_pipeline_diff.py` afterward too, or that rebuild's
changes won't be captured in the audit trail.

Does NOT run data/generate_source_data.py or scripts/load_bronze.py -- run
those first if you need fresh source data (same as before; unchanged).

Step 2's silver run: stg_crm_customers.sql / stg_erp_customers.sql are now
dbt `incremental` models (see those files' docstrings and
scripts/load_bronze.py's bronze.load_batches docstring) -- they merge only
the most recent bronze ingest batch instead of rescanning bronze's entire
history on every run. Before running step 2, this script reads
bronze.load_batches for the most recent batch's load_type and passes
`--full-refresh` to the silver dbt run whenever that batch was 'FULL'
(covers both a fresh clone's first-ever load and an explicit
`load_bronze.py --force-full`) -- an incremental batch runs without that
flag, letting dbt's `is_incremental()` merge just the new/changed rows. If
bronze.load_batches doesn't exist yet or has no rows (bronze has never
been loaded at all), this also forces --full-refresh, since dbt would
otherwise have nothing to merge into on a target table that doesn't exist
-- though in practice dbt already handles "target doesn't exist yet"
automatically regardless of this flag; this just avoids the pipeline
depending on that fallback by accident.

Usage:
    python scripts/generate_source_data.py
    python scripts/load_bronze.py
    python scripts/build_pipeline.py
"""
import os
import subprocess
import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"
DB_PATH = str(PROJECT_ROOT / "mdm_demo.duckdb")


def run(cmd, cwd=None, env=None):
    print(f"\n$ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, env=env)
    if result.returncode != 0:
        sys.exit(result.returncode)


def _latest_bronze_load_is_full() -> bool:
    """Reads bronze.load_batches (scripts/load_bronze.py) for the most
    recent ingest run's load_type. Defaults to True (force a full-refresh)
    if the table doesn't exist or is empty -- bronze has never been loaded,
    so there's nothing for an incremental merge to be relative to."""
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        exists = con.execute("""
            SELECT count(*) FROM information_schema.tables
            WHERE table_schema = 'bronze' AND table_name = 'load_batches'
        """).fetchone()[0]
        if not exists:
            return True
        row = con.execute(
            "SELECT load_type FROM bronze.load_batches ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        return row is None or row[0] == "FULL"
    finally:
        con.close()


def main():
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = "."

    # --no-partial-parse: forces a full reparse instead of trusting
    # target/partial_parse.msgpack. Without it, a manifest cache left over
    # from a previous run/OS/path can go stale and dbt raises a KeyError
    # looking up a macro file (e.g. 'mdm_demo://macros\\proper_case.sql')
    # that doesn't match the current parse. Cheap insurance, no downside.
    run([sys.executable, str(PROJECT_ROOT / "scripts" / "ensure_governed_schemas.py")])
    run([sys.executable, str(PROJECT_ROOT / "scripts" / "ensure_domain_metadata.py")])
    run(["dbt", "--no-partial-parse", "seed"], cwd=DBT_PROJECT_DIR, env=env)

    silver_cmd = ["dbt", "--no-partial-parse", "run", "--exclude", "gold.*"]
    if _latest_bronze_load_is_full():
        silver_cmd.append("--full-refresh")
    run(silver_cmd, cwd=DBT_PROJECT_DIR, env=env)

    run([sys.executable, str(PROJECT_ROOT / "scripts" / "cancel_stale_match_reviews.py")])
    run([sys.executable, str(PROJECT_ROOT / "scripts" / "generate_matches.py")])
    run(["dbt", "--no-partial-parse", "run", "--select", "gold.*"], cwd=DBT_PROJECT_DIR, env=env)
    run([sys.executable, str(PROJECT_ROOT / "scripts" / "audit_pipeline_diff.py")])

    print("\nPipeline build complete.")


if __name__ == "__main__":
    main()
