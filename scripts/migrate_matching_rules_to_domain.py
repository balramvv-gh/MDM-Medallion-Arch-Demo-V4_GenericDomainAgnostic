"""
Phase 3: migrates bus_rules.matching_rules / matching_thresholds /
survivorship_rules from implicitly-Customer-only tables to domain_key-scoped
ones -- the shape scripts/generate_matches.py (_domains_with_matching_config,
_load_matching_metadata) and dbt_project/models/gold/*.sql now read.

Additive at the column level (ADD COLUMN IF NOT EXISTS domain_key,
attribute_key/target_column stay as-is -- source_column on matching_rules is
NOT dropped, kept for reference/history, same convention as
scripts/migrate_column_rules_to_domain.py used for bus_rules.column_rules).

Unlike column_rules' migration, no canonical-name mapping table is needed
here: matching_rules.source_column and survivorship_rules.target_column
already ARE canonical attribute names (first_name, email, state_code, ...)
that match governance.domain_attributes.attribute_key 1:1 today -- an
identity backfill, not a lookup.

Idempotent: safe to re-run. Run once, standalone, after
scripts/ensure_domain_metadata.py and before the first Phase-3 run of
scripts/generate_matches.py.
"""
from pathlib import Path

import duckdb

DB_PATH = str(Path(__file__).resolve().parent.parent / "mdm_demo.duckdb")


def migrate(con):
    con.execute("ALTER TABLE bus_rules.matching_rules ADD COLUMN IF NOT EXISTS domain_key VARCHAR;")
    con.execute("ALTER TABLE bus_rules.matching_rules ADD COLUMN IF NOT EXISTS attribute_key VARCHAR;")
    con.execute("""
        UPDATE bus_rules.matching_rules
        SET domain_key = 'customer', attribute_key = source_column
        WHERE domain_key IS NULL
    """)

    con.execute("ALTER TABLE bus_rules.matching_thresholds ADD COLUMN IF NOT EXISTS domain_key VARCHAR;")
    con.execute("""
        UPDATE bus_rules.matching_thresholds
        SET domain_key = 'customer'
        WHERE domain_key IS NULL
    """)

    con.execute("ALTER TABLE bus_rules.survivorship_rules ADD COLUMN IF NOT EXISTS domain_key VARCHAR;")
    con.execute("ALTER TABLE bus_rules.survivorship_rules ADD COLUMN IF NOT EXISTS attribute_key VARCHAR;")
    con.execute("""
        UPDATE bus_rules.survivorship_rules
        SET domain_key = 'customer', attribute_key = target_column
        WHERE domain_key IS NULL
    """)

    counts = {
        "matching_rules": con.execute("SELECT count(*) FROM bus_rules.matching_rules WHERE domain_key IS NOT NULL").fetchone()[0],
        "matching_thresholds": con.execute("SELECT count(*) FROM bus_rules.matching_thresholds WHERE domain_key IS NOT NULL").fetchone()[0],
        "survivorship_rules": con.execute("SELECT count(*) FROM bus_rules.survivorship_rules WHERE domain_key IS NOT NULL").fetchone()[0],
    }
    return counts


def main():
    con = duckdb.connect(DB_PATH)
    counts = migrate(con)
    con.execute("CHECKPOINT")  # flush this run's ALTER/UPDATE out of the WAL -- see api/db.py's get_con()
    print("bus_rules.matching_rules / matching_thresholds / survivorship_rules migrated to domain_key-scoped:")
    for table, n in counts.items():
        print(f"  {table}: {n} row(s) with domain_key set")


if __name__ == "__main__":
    main()
