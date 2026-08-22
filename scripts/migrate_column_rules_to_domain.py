"""
Phase 2: migrates bus_rules.column_rules from (source_system, source_column)
keying to (domain_key, attribute_key) keying -- the shape
dbt_project/macros/column_rule_flags_generic.sql reads to generate
validation flags for ANY domain's silver staging, instead of the old
per-source-system column_rule_flags() macro that only ever ran against
Customer's fixed CRM/ERP shape.

Additive at the column level (ADD COLUMN IF NOT EXISTS domain_key,
attribute_key -- source_system/source_column are NOT dropped, kept for
history/reference), but this script also INSERTS one new rule, R016
(CRM phone required), that did not exist as a governed row before:
stg_crm_customers.sql previously hardcoded `(phone is null) as
err_missing_phone` unconditionally, outside the bus_rules.column_rules
mechanism entirely (see that file's docstring, pre-Phase-2). A model that's
generic across every domain/source can't contain a branch that
special-cases "CRM" by name, so this hardcoded check is promoted to a real
governed rule here -- the validation OUTCOME is identical (CRM records with
a null phone were always rejected, and still are), but it's now visible
and editable via Rules Configuration like every other rule, which it
previously was not. Flagged here explicitly rather than left as a silent
side effect of the schema migration.

Idempotent: safe to re-run. Run once, standalone, after
scripts/ensure_domain_metadata.py and before the first Phase-2 dbt build.
"""
from pathlib import Path

import duckdb

DB_PATH = str(Path(__file__).resolve().parent.parent / "mdm_demo.duckdb")

# (source_system, source_column) -> (domain_key, attribute_key), covering
# every existing R001-R015 row. Mirrors api/validation.py's
# CANONICAL_FIELD_MAP (pre-Phase-2) -- same mapping, just now written back
# into the rules table itself instead of living only in Python.
_CANONICAL_MAP = {
    ("CRM", "first_name"): "first_name",
    ("CRM", "last_name"): "last_name",
    ("CRM", "email"): "email",
    ("CRM", "state"): "state_code",
    ("CRM", "country"): "country_code",
    ("CRM", "phone"): "phone",
    ("ERP", "email_addr"): "email",
    ("ERP", "contact_phone"): "phone",
    ("ERP", "state_code"): "state_code",
    ("ERP", "country_code"): "country_code",
    # ERP's full_name (R009) intentionally has NO entry here -- see below.
}

# R016: promotes CRM's previously-hardcoded "phone is required" check
# (stg_crm_customers.sql's old `(phone is null) as err_missing_phone`,
# never governed by any bus_rules row) into a real rule. Reproduces the
# exact same validation outcome; see module docstring.
_NEW_RULES = [
    ("R016", "CRM", "phone", "not_null", None, "reject",
     "Phone is required (promoted from a pre-Phase-2 hardcoded check -- "
     "see scripts/migrate_column_rules_to_domain.py)."),
]


def migrate(con):
    con.execute("ALTER TABLE bus_rules.column_rules ADD COLUMN IF NOT EXISTS domain_key VARCHAR;")
    con.execute("ALTER TABLE bus_rules.column_rules ADD COLUMN IF NOT EXISTS attribute_key VARCHAR;")

    rows = con.execute(
        "SELECT rule_id, source_system, source_column FROM bus_rules.column_rules"
    ).fetchall()
    unmapped = []
    for rule_id, source_system, source_column in rows:
        attribute_key = _CANONICAL_MAP.get((source_system, source_column))
        if attribute_key is None:
            # R009 (ERP full_name): a composite check spanning two derived
            # attributes, not a plain "does this attribute violate this
            # rule" check -- deliberately left NOT attribute-key-mapped
            # here (same "deliberately not generalized" scope
            # PROJECT_KNOWLEDGE.md already documents for this exact rule
            # pre-Phase-2). It stays hand-written in
            # stg_source_records.sql and is simply not read by
            # column_rule_flags_generic() (that macro skips rows with a
            # NULL attribute_key).
            unmapped.append((rule_id, source_system, source_column))
            con.execute(
                "UPDATE bus_rules.column_rules SET domain_key = 'customer' WHERE rule_id = ?",
                [rule_id],
            )
            continue
        con.execute(
            "UPDATE bus_rules.column_rules SET domain_key = 'customer', attribute_key = ? WHERE rule_id = ?",
            [attribute_key, rule_id],
        )

    existing_ids = set(r[0] for r in con.execute("SELECT rule_id FROM bus_rules.column_rules").fetchall())
    added = 0
    for rule_id, source_system, attribute_key, rule_type, rule_param, severity, description in _NEW_RULES:
        if rule_id in existing_ids:
            continue
        con.execute("""
            INSERT INTO bus_rules.column_rules
                (rule_id, domain_key, source_system, attribute_key, source_column,
                 rule_type, rule_param, severity, description)
            VALUES (?, 'customer', ?, ?, ?, ?, ?, ?, ?)
        """, (rule_id, source_system, attribute_key, attribute_key, rule_type, rule_param, severity, description))
        added += 1

    return unmapped, added


def main():
    con = duckdb.connect(DB_PATH)
    unmapped, added = migrate(con)
    con.execute("CHECKPOINT")  # flush this run's ALTER/UPDATE out of the WAL -- see api/db.py's get_con()
    print(f"bus_rules.column_rules migrated to (domain_key, attribute_key). "
          f"{added} new rule(s) added.")
    if unmapped:
        print("Rows intentionally left without an attribute_key (composite/hand-written checks):")
        for rule_id, source_system, source_column in unmapped:
            print(f"  {rule_id} ({source_system}.{source_column})")


if __name__ == "__main__":
    main()
