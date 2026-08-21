"""
Provisions the 'governance.domain_registry' / 'domain_attributes' /
'domain_source_systems' / 'domain_relationships' / 'user_domain_access'
tables -- the metadata layer that makes V4 domain-agnostic (see
PROJECT_KNOWLEDGE.md's domain-agnostic architecture section). Same
"CREATE TABLE IF NOT EXISTS + seed only if empty/missing" convention as
ensure_governed_schemas.py, and the same api/db.py duplication convention
(see that file's module docstring) -- api/db.py's _ensure_domain_tables
must be kept in sync with this file's DDL.

Phase 1 is additive-only: it seeds domain_registry with 'customer'
(pipeline_status='live', reflecting the existing, real bronze/silver/gold
Customer pipeline) and 'product' (pipeline_status='planned', a metadata-only
placeholder so the new Home relationship diagram has a second domain/link to
show -- selecting it in the UI surfaces "not yet available", no real
pipeline behind it yet). domain_attributes for 'customer' mirrors today's
silver_customers/gold_customers column set 1:1. This script does not change
any existing table's shape or any pipeline behavior; it only adds new
tables and migrates each auth.users.gold_access value into a parallel
governance.user_domain_access row scoped to domain_key='customer' -- both
stay live side by side until Phase 4 cuts the API's read path over to
user_domain_access.

Standalone script (not just api/db.py) for the same reason
ensure_governed_schemas.py is standalone: scripts/build_pipeline.py needs
this metadata to exist before dbt runs, which can be before the FastAPI app
has ever started on a fresh clone.
"""
from pathlib import Path

import duckdb

DB_PATH = str(Path(__file__).resolve().parent.parent / "mdm_demo.duckdb")


# Domain registry seed. pipeline_status: 'live' = a real bronze/silver/gold
# pipeline exists and is tested; 'planned' = metadata-only placeholder (no
# bronze/silver/gold data), shown on the Home relationship diagram but not
# selectable as a working domain yet.
DEFAULT_DOMAINS = [
    # domain_key, display_name, entity_label_singular, entity_label_plural,
    # description, pipeline_status, is_active, sort_order
    ("customer", "Customer", "Customer", "Customers",
     "Customer master data, matched/merged from CRM and ERP source systems.",
     "live", True, 1),
    ("product", "Product", "Product", "Products",
     "Product master data (placeholder domain -- metadata only, no pipeline "
     "yet; demonstrates that the platform is not hardcoded to a single "
     "domain).",
     "planned", True, 2),
]

# Canonical attribute set for domain_key='customer', taken 1:1 from today's
# silver_customers/gold_customers column set. is_matching_field /
# is_similarity_text_field / is_blocking_key mirror today's
# bus_rules.matching_rules rows (MR001-MR008) as descriptive domain
# metadata; the matching pipeline itself still reads bus_rules.matching_rules
# directly (Phase 3 wires the two together properly).
DEFAULT_CUSTOMER_ATTRIBUTES = [
    # attribute_key, display_label, data_type, is_required, is_matching_field,
    # is_similarity_text_field, is_blocking_key, sort_order
    ("first_name", "First Name", "string", True, False, True, False, 1),
    ("last_name", "Last Name", "string", True, False, True, False, 2),
    ("email", "Email", "string", True, True, False, False, 3),
    ("phone", "Phone", "string", False, True, False, False, 4),
    ("address_line1", "Address Line 1", "string", False, False, True, False, 5),
    ("address_line2", "Address Line 2", "string", False, False, True, False, 6),
    ("city", "City", "string", False, False, True, False, 7),
    ("state_code", "State", "string", True, False, False, True, 8),
    ("postal_code", "Postal Code", "string", False, False, False, False, 9),
    ("country_code", "Country", "string", True, False, False, False, 10),
    ("source_created_date", "Source Created Date", "date", False, False, False, False, 11),
    ("source_modified_date", "Source Modified Date", "date", False, False, False, False, 12),
]

DEFAULT_CUSTOMER_SOURCE_SYSTEMS = [
    # domain_key, source_system_key, display_name, sort_order
    ("customer", "CRM", "CRM", 1),
    ("customer", "ERP", "ERP", 2),
]

DEFAULT_DOMAIN_RELATIONSHIPS = [
    # relationship_id, domain_a_key, domain_b_key, relationship_label, description
    ("REL001", "customer", "product", "Customer-Product link",
     "Illustrative link between the Customer and Product domains (e.g. "
     "purchase history) -- Product has no pipeline yet, so this "
     "relationship is metadata only for now."),
]


def ensure_domain_tables(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS governance;")

    con.execute("""
        CREATE TABLE IF NOT EXISTS governance.domain_registry (
            domain_key VARCHAR PRIMARY KEY,
            display_name VARCHAR,
            entity_label_singular VARCHAR,
            entity_label_plural VARCHAR,
            description VARCHAR,
            pipeline_status VARCHAR DEFAULT 'planned',  -- 'live' | 'planned'
            is_active BOOLEAN DEFAULT true,
            sort_order INTEGER,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS governance.domain_attributes (
            domain_key VARCHAR,
            attribute_key VARCHAR,
            display_label VARCHAR,
            data_type VARCHAR,               -- 'string' | 'date' | 'number' | 'boolean'
            is_required BOOLEAN DEFAULT false,
            is_matching_field BOOLEAN DEFAULT false,
            is_similarity_text_field BOOLEAN DEFAULT false,
            is_blocking_key BOOLEAN DEFAULT false,
            sort_order INTEGER,
            active BOOLEAN DEFAULT true,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR,
            PRIMARY KEY (domain_key, attribute_key)
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS governance.domain_source_systems (
            domain_key VARCHAR,
            source_system_key VARCHAR,
            display_name VARCHAR,
            sort_order INTEGER,
            active BOOLEAN DEFAULT true,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR,
            PRIMARY KEY (domain_key, source_system_key)
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS governance.domain_relationships (
            relationship_id VARCHAR PRIMARY KEY,
            domain_a_key VARCHAR,
            domain_b_key VARCHAR,
            relationship_label VARCHAR,
            description VARCHAR,
            is_active BOOLEAN DEFAULT true,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS governance.user_domain_access (
            user_id VARCHAR,
            domain_key VARCHAR,
            gold_access VARCHAR DEFAULT 'none',  -- 'read_write' | 'read' | 'none'
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR,
            PRIMARY KEY (user_id, domain_key)
        );
    """)

    # Per-key existence check (not whole-table-empty), same convention as
    # entity_type_registry -- safe to introduce into an already-running DB.
    existing_domains = set(
        r[0] for r in con.execute("SELECT domain_key FROM governance.domain_registry").fetchall()
    )
    for row in DEFAULT_DOMAINS:
        if row[0] in existing_domains:
            continue
        con.execute("""
            INSERT INTO governance.domain_registry
                (domain_key, display_name, entity_label_singular, entity_label_plural,
                 description, pipeline_status, is_active, sort_order)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, row)

    existing_attrs = set(
        (r[0], r[1]) for r in con.execute(
            "SELECT domain_key, attribute_key FROM governance.domain_attributes"
        ).fetchall()
    )
    for attr in DEFAULT_CUSTOMER_ATTRIBUTES:
        if ("customer", attr[0]) in existing_attrs:
            continue
        con.execute("""
            INSERT INTO governance.domain_attributes
                (domain_key, attribute_key, display_label, data_type, is_required,
                 is_matching_field, is_similarity_text_field, is_blocking_key, sort_order)
            VALUES ('customer', ?, ?, ?, ?, ?, ?, ?, ?)
        """, attr)

    existing_sources = set(
        (r[0], r[1]) for r in con.execute(
            "SELECT domain_key, source_system_key FROM governance.domain_source_systems"
        ).fetchall()
    )
    for row in DEFAULT_CUSTOMER_SOURCE_SYSTEMS:
        if (row[0], row[1]) in existing_sources:
            continue
        con.execute("""
            INSERT INTO governance.domain_source_systems
                (domain_key, source_system_key, display_name, sort_order)
            VALUES (?, ?, ?, ?)
        """, row)

    existing_rels = set(
        r[0] for r in con.execute("SELECT relationship_id FROM governance.domain_relationships").fetchall()
    )
    for row in DEFAULT_DOMAIN_RELATIONSHIPS:
        if row[0] in existing_rels:
            continue
        con.execute("""
            INSERT INTO governance.domain_relationships
                (relationship_id, domain_a_key, domain_b_key, relationship_label, description)
            VALUES (?, ?, ?, ?, ?)
        """, row)


def migrate_user_domain_access(con):
    """One-time, idempotent migration: copy each auth.users.gold_access value
    into a governance.user_domain_access row scoped to domain_key='customer'
    -- every existing user's access to the one domain that exists today
    carries forward unchanged. Safe to re-run: a (user_id, domain_key) pair
    that already has a row is left untouched, so a governance user's later,
    domain-specific tuning here is never clobbered by re-running this
    script. auth.users.gold_access itself is NOT dropped or modified by this
    migration -- it keeps working as today's read path until Phase 4 cuts
    the API over to reading user_domain_access instead; the two are NOT kept
    in sync on every future change, only backfilled once here (a user's
    gold_access edited after this point will not auto-propagate to
    user_domain_access until Phase 4 removes gold_access as the read path
    entirely)."""
    users = con.execute("SELECT user_id, gold_access FROM auth.users").fetchall()
    existing = set(
        (r[0], r[1]) for r in con.execute(
            "SELECT user_id, domain_key FROM governance.user_domain_access"
        ).fetchall()
    )
    migrated = 0
    for user_id, gold_access in users:
        if (user_id, "customer") in existing:
            continue
        con.execute("""
            INSERT INTO governance.user_domain_access (user_id, domain_key, gold_access)
            VALUES (?, 'customer', ?)
        """, (user_id, gold_access or "none"))
        migrated += 1
    return migrated


def main():
    con = duckdb.connect(DB_PATH)
    ensure_domain_tables(con)
    migrated = migrate_user_domain_access(con)
    print(
        "governance.domain_registry/domain_attributes/domain_source_systems/"
        "domain_relationships/user_domain_access ready. "
        f"{migrated} user(s) newly backfilled into user_domain_access."
    )


if __name__ == "__main__":
    main()
