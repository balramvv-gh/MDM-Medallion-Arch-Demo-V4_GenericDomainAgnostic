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
Customer pipeline) and 'product' (pipeline_status='live' as of Product
onboarding -- see below; originally shipped as 'planned', a metadata-only
placeholder). domain_attributes for 'customer' mirrors today's
silver_customers/gold_customers column set 1:1. This script does not change
any existing table's shape or any pipeline behavior; it only adds new
tables and migrates each auth.users.gold_access value into a parallel
governance.user_domain_access row scoped to domain_key='customer' -- both
stay live side by side until Phase 4 cuts the API's read path over to
user_domain_access.

Product domain onboarding: three synthetic source systems (ERP -- commercial
+ operational attributes, PLM -- engineering attributes, SUPPLIER -- the
supplier's own spec sheet), each with genuinely different raw field names
for the same canonical attributes (mirroring the CRM/ERP naming divergence
already established for Customer), asymmetric field coverage (e.g. only ERP
carries list_price/currency_code; only PLM carries material/color/
engineering_spec_version), and one deliberate data-type/format mismatch
(ERP's gross_weight arrives as free text with a unit, "4.20 kg"; PLM's
weight_kg is already a clean number) resolved via the new 'numeric_extract'
source_field_mappings.transform_function (see dbt_project/macros/
transform_expr.sql). Onboarded the same additive-only way Customer was --
adding rows here, never editing a dbt model (see macros/active_domain_sources.sql,
mapped_attributes.sql, validation_block.sql).

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
     "Product master data, matched/merged from ERP (commercial + operational), "
     "PLM (engineering), and Supplier (the supplier's own spec sheet) source systems.",
     "live", True, 2),
]

# Existing installs may already have a 'product' row seeded back when it was
# still pipeline_status='planned' (metadata-only placeholder) -- the
# insert-if-missing loop below never touches an existing row, so onboarding
# Product's real pipeline needs an explicit, idempotent flip alongside it.
# Safe to re-run: a no-op once already 'live'.
_PRODUCT_PIPELINE_STATUS_UPGRADE_SQL = """
    UPDATE governance.domain_registry
    SET pipeline_status = 'live',
        description = 'Product master data, matched/merged from ERP (commercial + '
                       'operational), PLM (engineering), and Supplier (the supplier''s '
                       'own spec sheet) source systems.'
    WHERE domain_key = 'product' AND pipeline_status != 'live'
"""

# Canonical attribute set for domain_key='customer', taken 1:1 from today's
# silver_customers/gold_customers column set. is_matching_field /
# is_similarity_text_field / is_blocking_key mirror today's
# bus_rules.matching_rules rows (MR001-MR008) as descriptive domain
# metadata; the matching pipeline itself still reads bus_rules.matching_rules
# directly (Phase 3 wires the two together properly).
#
# is_dashboard_breakdown (Phase 4, domain-agnostic): flags which attributes
# the portal dashboard (GET /api/v1/portal/dashboard/{domain_key}/summary)
# renders a "count by value" chart for -- replaces the old hardcoded
# by_state/by_country breakdowns in api/main.py with a generic loop over
# every attribute flagged here, for any domain. Only state_code/
# country_code are flagged for customer today (matching the pre-Phase-4
# dashboard's exact two breakdowns); a future domain enables its own by
# flagging different attributes -- no API code change needed.
DEFAULT_CUSTOMER_ATTRIBUTES = [
    # attribute_key, display_label, data_type, is_required, is_matching_field,
    # is_similarity_text_field, is_blocking_key, is_dashboard_breakdown, sort_order
    ("first_name", "First Name", "string", True, False, True, False, False, 1),
    ("last_name", "Last Name", "string", True, False, True, False, False, 2),
    ("email", "Email", "string", True, True, False, False, False, 3),
    ("phone", "Phone", "string", False, True, False, False, False, 4),
    ("address_line1", "Address Line 1", "string", False, False, True, False, False, 5),
    ("address_line2", "Address Line 2", "string", False, False, True, False, False, 6),
    ("city", "City", "string", False, False, True, False, False, 7),
    ("state_code", "State", "string", True, False, False, True, True, 8),
    ("postal_code", "Postal Code", "string", False, False, False, False, False, 9),
    ("country_code", "Country", "string", True, False, False, False, True, 10),
    ("source_created_date", "Source Created Date", "date", False, False, False, False, False, 11),
    ("source_modified_date", "Source Modified Date", "date", False, False, False, False, False, 12),
]

# Canonical attribute set for domain_key='product'. is_matching_field is set
# only on manufacturer_part_number (tier-1 exact match key, see
# bus_rules.matching_rules MR100); is_similarity_text_field on product_name/
# manufacturer_name (tier-2 fuzzy similarity text, MR101/MR102);
# is_blocking_key on category (MR103) -- deliberately mirroring how Customer's
# email/phone vs. first_name/last_name/address vs. state_code are flagged.
# is_dashboard_breakdown on category/product_status (the two most useful
# "count by value" breakdowns for a product catalog, same count as Customer's
# state_code/country_code).
DEFAULT_PRODUCT_ATTRIBUTES = [
    # attribute_key, display_label, data_type, is_required, is_matching_field,
    # is_similarity_text_field, is_blocking_key, is_dashboard_breakdown, sort_order
    ("product_name", "Product Name", "string", True, False, True, False, False, 1),
    ("manufacturer_part_number", "Manufacturer Part Number", "string", True, True, False, False, False, 2),
    ("manufacturer_name", "Manufacturer", "string", False, False, True, False, False, 3),
    ("category", "Product Category", "string", True, False, False, True, True, 4),
    ("unit_of_measure", "Unit of Measure", "string", False, False, False, False, False, 5),
    ("list_price", "List Price", "number", False, False, False, False, False, 6),
    ("currency_code", "Currency", "string", False, False, False, False, False, 7),
    ("product_status", "Status", "string", True, False, False, False, True, 8),
    ("lifecycle_status", "Engineering Lifecycle Status", "string", False, False, False, False, False, 9),
    ("weight_kg", "Weight (kg)", "number", False, False, False, False, False, 10),
    ("material", "Material", "string", False, False, False, False, False, 11),
    ("color", "Color", "string", False, False, False, False, False, 12),
    ("country_of_origin", "Country of Origin", "string", False, False, False, False, False, 13),
    ("supplier_id", "Supplier ID", "string", True, False, False, False, False, 14),
    ("supplier_part_number", "Supplier Part Number", "string", False, False, False, False, False, 15),
    ("lead_time_days", "Lead Time (Days)", "number", False, False, False, False, False, 16),
    ("minimum_order_qty", "Minimum Order Qty", "number", False, False, False, False, False, 17),
    ("engineering_spec_version", "Engineering Spec Version", "string", False, False, False, False, False, 18),
    ("source_created_date", "Source Created Date", "date", False, False, False, False, False, 19),
    ("source_modified_date", "Source Modified Date", "date", False, False, False, False, False, 20),
]

# (domain_key, attribute_key) tuples flagged with is_dashboard_breakdown -- used
# both by the insert loop below and by the idempotent backfill UPDATE, so the
# two can never drift from each other.
_DASHBOARD_BREAKDOWN_ATTRS = {
    ("customer", "state_code"), ("customer", "country_code"),
    ("product", "category"), ("product", "product_status"),
}

# All domains' attribute rows, tagged with their domain_key -- generalizes the
# insert loop below from "always domain_key='customer'" to "whatever domain_key
# this row says", the same shape DEFAULT_CUSTOMER_SOURCE_SYSTEMS/
# DEFAULT_SOURCE_FIELD_MAPPINGS already used (their own domain_key is just the
# tuple's own first element).
ALL_DOMAIN_ATTRIBUTES = (
    [("customer",) + attr for attr in DEFAULT_CUSTOMER_ATTRIBUTES]
    + [("product",) + attr for attr in DEFAULT_PRODUCT_ATTRIBUTES]
)

DEFAULT_CUSTOMER_SOURCE_SYSTEMS = [
    # domain_key, source_system_key, display_name, sort_order
    ("customer", "CRM", "CRM", 1),
    ("customer", "ERP", "ERP", 2),
]

# Product's three source systems. Lower sort_order wins survivorship's
# universal tie-break (governance.domain_source_systems.sort_order) whenever a
# bus_rules.survivorship_rules rule doesn't produce a clean winner -- ERP first
# since it's this demo's commercial system of record, mirroring the
# "CRM preferred" -> sort_order convention already established for Customer.
DEFAULT_PRODUCT_SOURCE_SYSTEMS = [
    ("product", "ERP", "ERP", 1),
    ("product", "PLM", "PLM", 2),
    ("product", "SUPPLIER", "Supplier", 3),
]

DEFAULT_DOMAIN_RELATIONSHIPS = [
    # relationship_id, domain_a_key, domain_b_key, relationship_label, description
    ("REL001", "customer", "product", "Customer-Product link",
     "Illustrative link between the Customer and Product domains (e.g. "
     "purchase history)."),
]

# ---------------------------------------------------------------------------
# Phase 2: governance.source_field_mappings -- how each domain's source
# system's own raw field names (bronze.raw_records.attributes, landed
# verbatim/untouched) map onto that domain's canonical attribute_keys, plus
# which standardization transform to apply. Read by
# dbt_project/macros/mapped_attributes.sql at compile time (same "run_query()
# over a governed table" pattern as column_rule_flags.sql) to generate
# stg_source_records.sql's per-(domain,source_system) extraction block -- one
# generic model for every domain/source instead of a hand-written dbt file
# per source system.
#
# source_expr is a SQL expression over `attributes` (bronze's raw JSON
# column) -- almost always a plain `attributes->>'raw_col'` extraction, but
# may be a richer expression for a source's own quirks (e.g. ERP's
# "Last, First" combined name field needs split_part(...)). This mirrors
# real-world MDM connector configuration: mapping a new source's fields
# always requires some source-specific adapter logic (every ETL/MDM
# platform needs this), but everything downstream of this mapping step --
# validation, matching, survivorship, the UI -- is 100% generic and
# metadata-driven, with no domain or source name in any code.
#
# transform_function dispatches through
# dbt_project/macros/transform_expr.sql (mirrors rule_type_expr.sql):
# 'none' | 'trim' | 'trim_proper_case' | 'upper_trim' | 'lowercase_trim' |
# 'phone_e164_us' | 'numeric_extract'.
# ---------------------------------------------------------------------------

DEFAULT_SOURCE_FIELD_MAPPINGS = [
    # mapping_id, domain_key, source_system_key, attribute_key, source_expr, transform_function, description
    ("SFM001", "customer", "CRM", "first_name", "attributes->>'first_name'", "trim_proper_case",
     "CRM's first_name column, trimmed and proper-cased (R006/R007)."),
    ("SFM002", "customer", "CRM", "last_name", "attributes->>'last_name'", "trim_proper_case",
     "CRM's last_name column, trimmed and proper-cased (R006/R007)."),
    ("SFM003", "customer", "CRM", "email", "attributes->>'email'", "trim",
     "CRM's email column, trimmed only -- NOT lowercased (matches pre-Phase-2 behavior; ERP's is)."),
    ("SFM004", "customer", "CRM", "phone", "attributes->>'phone'", "phone_e164_us",
     "CRM's phone column, standardized to +1XXXXXXXXXX (R008)."),
    ("SFM005", "customer", "CRM", "address_line1", "attributes->>'address_line1'", "trim", None),
    ("SFM006", "customer", "CRM", "city", "attributes->>'city'", "trim", None),
    ("SFM007", "customer", "CRM", "state_code", "attributes->>'state'", "upper_trim",
     "CRM's raw column is named 'state'; canonical attribute is state_code."),
    ("SFM008", "customer", "CRM", "postal_code", "attributes->>'zip'", "trim",
     "CRM's raw column is named 'zip'; canonical attribute is postal_code."),
    ("SFM009", "customer", "CRM", "country_code", "attributes->>'country'", "upper_trim",
     "CRM's raw column is named 'country'; canonical attribute is country_code."),
    (
        "SFM010", "customer", "ERP", "first_name",
        "split_part(attributes->>'full_name', ',', 2)", "trim_proper_case",
        "ERP's full_name arrives as \"Last, First\" -- second comma-part is the first name.",
    ),
    (
        "SFM011", "customer", "ERP", "last_name",
        "split_part(attributes->>'full_name', ',', 1)", "trim_proper_case",
        "ERP's full_name arrives as \"Last, First\" -- first comma-part is the last name.",
    ),
    ("SFM012", "customer", "ERP", "email", "attributes->>'email_addr'", "lowercase_trim",
     "ERP's email_addr column, lowercased and trimmed (R014)."),
    ("SFM013", "customer", "ERP", "phone", "attributes->>'contact_phone'", "phone_e164_us",
     "ERP's contact_phone column, standardized to +1XXXXXXXXXX (R015)."),
    ("SFM014", "customer", "ERP", "address_line1", "attributes->>'addr1'", "trim", None),
    ("SFM015", "customer", "ERP", "address_line2", "attributes->>'addr2'", "trim", None),
    ("SFM016", "customer", "ERP", "city", "attributes->>'city_code'", "trim", None),
    ("SFM017", "customer", "ERP", "state_code", "attributes->>'state_code'", "upper_trim", None),
    ("SFM018", "customer", "ERP", "postal_code", "attributes->>'postal_code'", "trim", None),
    ("SFM019", "customer", "ERP", "country_code", "attributes->>'country_code'", "upper_trim", None),
    # source_created_date / source_modified_date: structural "when did the
    # source record last change" fields (domain_attributes sort_order 11/12),
    # mapped the same way as any other attribute -- transform_function 'none'
    # means "pass the raw value through, only the attribute's data_type-based
    # cast applies" (see dbt_project/macros/transform_expr.sql). ERP has no
    # created-date field at all, so it deliberately has NO SFM row for
    # source_created_date -- mapped_attributes() emits NULL for a
    # (domain, source, attribute) combination with no mapping row, matching
    # pre-Phase-2 stg_erp_customers.sql's `cast(null as date) as source_created_date`.
    ("SFM020", "customer", "CRM", "source_created_date", "attributes->>'created_date'", "none", None),
    ("SFM021", "customer", "CRM", "source_modified_date", "attributes->>'modified_date'", "none", None),
    ("SFM022", "customer", "ERP", "source_modified_date", "attributes->>'last_updated'", "none", None),
]

# Product domain: three sources with genuinely different raw field names for
# the same canonical attribute (mirroring CRM's "state"/ERP's "state_code"
# divergence, now across three systems) and asymmetric coverage -- ERP alone
# carries list_price/currency_code/product_status/supplier_part_number
# (cross-ref copy); PLM alone carries manufacturer_name/lifecycle_status/
# material/color/country_of_origin/engineering_spec_version; SUPPLIER alone
# carries lead_time_days/minimum_order_qty. manufacturer_part_number and
# category ARE mapped by all three (they're the matching/blocking keys and
# need to exist everywhere for matching to work), but each source's raw
# column name and, in the generated data, occasional value quality still
# differ -- see data/generate_product_source_data.py.
DEFAULT_PRODUCT_SOURCE_FIELD_MAPPINGS = [
    # ERP: item_number is ERP's own internal key (bronze's source_record_id,
    # not a mapped attribute) -- everything else below is commercial/operational.
    ("SFM100", "product", "ERP", "product_name", "attributes->>'item_description'", "trim", None),
    ("SFM101", "product", "ERP", "manufacturer_part_number", "attributes->>'mfr_part_no'", "upper_trim",
     "ERP's mfr_part_no column, upper-cased for consistent display -- matching normalizes "
     "further at match time via matching_rules' 'normalize_code' (MR100)."),
    ("SFM102", "product", "ERP", "category", "attributes->>'category_code'", "upper_trim",
     "ERP's category_code column."),
    ("SFM103", "product", "ERP", "unit_of_measure", "attributes->>'uom'", "upper_trim", None),
    ("SFM104", "product", "ERP", "list_price", "attributes->>'unit_price'", "trim", None),
    ("SFM105", "product", "ERP", "currency_code", "attributes->>'currency'", "upper_trim", None),
    ("SFM106", "product", "ERP", "product_status", "attributes->>'item_status'", "trim_proper_case", None),
    ("SFM107", "product", "ERP", "weight_kg", "attributes->>'gross_weight'", "numeric_extract",
     "ERP's gross_weight column mixes a unit into the same text field (e.g. \"4.20 kg\") -- "
     "numeric_extract pulls just the number, unlike PLM's already-clean weight_kg (SFM118)."),
    ("SFM108", "product", "ERP", "supplier_id", "attributes->>'vendor_id'", "upper_trim",
     "ERP's own internal vendor code -- deliberately not always the same format as Supplier's "
     "own vendor_account_id for the same real-world supplier (SFM132)."),
    ("SFM109", "product", "ERP", "supplier_part_number", "attributes->>'vendor_part_no'", "upper_trim",
     "ERP's own cross-reference copy of the supplier's part number -- occasionally stale/"
     "mismatched against Supplier's own record (SFM133)."),
    ("SFM110", "product", "ERP", "source_created_date", "attributes->>'created_on'", "none", None),
    ("SFM111", "product", "ERP", "source_modified_date", "attributes->>'last_modified'", "none", None),

    # PLM: part_id is PLM's own internal key (source_record_id) -- everything
    # else is engineering.
    ("SFM112", "product", "PLM", "product_name", "attributes->>'part_name'", "trim", None),
    ("SFM113", "product", "PLM", "manufacturer_part_number", "attributes->>'mpn'", "upper_trim", None),
    ("SFM114", "product", "PLM", "manufacturer_name", "attributes->>'mfr_name'", "trim_proper_case", None),
    ("SFM115", "product", "PLM", "category", "attributes->>'product_family'", "upper_trim",
     "PLM's product_family column."),
    ("SFM116", "product", "PLM", "unit_of_measure", "attributes->>'uom_code'", "upper_trim", None),
    ("SFM117", "product", "PLM", "lifecycle_status", "attributes->>'lifecycle_stage'", "trim_proper_case", None),
    ("SFM118", "product", "PLM", "weight_kg", "attributes->>'weight_kg'", "trim",
     "PLM's own weight_kg column is already a clean number -- no unit text to strip, unlike "
     "ERP's gross_weight (SFM107)."),
    ("SFM119", "product", "PLM", "material", "attributes->>'material_spec'", "trim_proper_case", None),
    ("SFM120", "product", "PLM", "color", "attributes->>'color_spec'", "trim_proper_case", None),
    ("SFM121", "product", "PLM", "country_of_origin", "attributes->>'origin_country'", "upper_trim", None),
    ("SFM122", "product", "PLM", "engineering_spec_version", "attributes->>'spec_rev'", "upper_trim", None),
    ("SFM123", "product", "PLM", "source_created_date", "attributes->>'date_created'", "none", None),
    ("SFM124", "product", "PLM", "source_modified_date", "attributes->>'date_modified'", "none", None),

    # SUPPLIER: supplier_sku is the supplier's own internal key (source_record_id)
    # -- everything else is the supplier's own spec sheet (landed from a
    # spreadsheet extract, see scripts/load_bronze.py's file_type='xlsx' entry).
    ("SFM125", "product", "SUPPLIER", "product_name", "attributes->>'product_title'", "trim", None),
    ("SFM126", "product", "SUPPLIER", "manufacturer_part_number", "attributes->>'cross_ref_mpn'", "upper_trim",
     "The supplier's own cross-reference to the MPN -- sometimes blank or mismatched, since "
     "it's a value they copy in manually rather than the authoritative PLM source (SFM113)."),
    ("SFM127", "product", "SUPPLIER", "manufacturer_name", "attributes->>'mfg_name'", "trim_proper_case", None),
    ("SFM128", "product", "SUPPLIER", "category", "attributes->>'commodity_code'", "upper_trim",
     "Supplier's own commodity_code column."),
    ("SFM129", "product", "SUPPLIER", "material", "attributes->>'material_type'", "trim_proper_case", None),
    ("SFM130", "product", "SUPPLIER", "color", "attributes->>'color_desc'", "trim_proper_case", None),
    ("SFM131", "product", "SUPPLIER", "country_of_origin", "attributes->>'country_of_mfg'", "upper_trim", None),
    ("SFM132", "product", "SUPPLIER", "supplier_id", "attributes->>'vendor_account_id'", "upper_trim",
     "The supplier's own account/vendor id -- deliberately a different code scheme than ERP's "
     "vendor_id for the same real-world supplier in some rows (SFM108)."),
    ("SFM133", "product", "SUPPLIER", "supplier_part_number", "attributes->>'supplier_part_no'", "upper_trim", None),
    ("SFM134", "product", "SUPPLIER", "lead_time_days", "attributes->>'lead_time'", "trim", None),
    ("SFM135", "product", "SUPPLIER", "minimum_order_qty", "attributes->>'moq'", "trim", None),
    ("SFM136", "product", "SUPPLIER", "source_created_date", "attributes->>'date_added'", "none", None),
    ("SFM137", "product", "SUPPLIER", "source_modified_date", "attributes->>'date_updated'", "none", None),
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
    # Additive column on an already-existing table (a DB created before this
    # flag existed) -- see DEFAULT_CUSTOMER_ATTRIBUTES' docstring above.
    con.execute("ALTER TABLE governance.domain_attributes ADD COLUMN IF NOT EXISTS is_dashboard_breakdown BOOLEAN DEFAULT false;")
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

    # Idempotent upgrade for an existing install that already seeded 'product'
    # back when it was pipeline_status='planned' -- see
    # _PRODUCT_PIPELINE_STATUS_UPGRADE_SQL's own comment above.
    con.execute(_PRODUCT_PIPELINE_STATUS_UPGRADE_SQL)

    existing_attrs = set(
        (r[0], r[1]) for r in con.execute(
            "SELECT domain_key, attribute_key FROM governance.domain_attributes"
        ).fetchall()
    )
    for domain_key, *attr in ALL_DOMAIN_ATTRIBUTES:
        if (domain_key, attr[0]) in existing_attrs:
            continue
        con.execute("""
            INSERT INTO governance.domain_attributes
                (domain_key, attribute_key, display_label, data_type, is_required,
                 is_matching_field, is_similarity_text_field, is_blocking_key,
                 is_dashboard_breakdown, sort_order)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [domain_key] + attr)

    # Backfill is_dashboard_breakdown onto rows inserted by an earlier run
    # (before this flag existed for that domain) -- the insert-if-missing loop
    # above only covers brand-new rows, not a flag value changed afterward on
    # an already-existing one. Idempotent: a no-op once already applied.
    for domain_key, attribute_key in _DASHBOARD_BREAKDOWN_ATTRS:
        con.execute("""
            UPDATE governance.domain_attributes
            SET is_dashboard_breakdown = true
            WHERE domain_key = ? AND attribute_key = ? AND NOT is_dashboard_breakdown
        """, [domain_key, attribute_key])

    existing_sources = set(
        (r[0], r[1]) for r in con.execute(
            "SELECT domain_key, source_system_key FROM governance.domain_source_systems"
        ).fetchall()
    )
    for row in DEFAULT_CUSTOMER_SOURCE_SYSTEMS + DEFAULT_PRODUCT_SOURCE_SYSTEMS:
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

    con.execute("""
        CREATE TABLE IF NOT EXISTS governance.source_field_mappings (
            mapping_id VARCHAR PRIMARY KEY,
            domain_key VARCHAR,
            source_system_key VARCHAR,
            attribute_key VARCHAR,
            source_expr VARCHAR,      -- SQL expression over bronze's raw `attributes` JSON column
            transform_function VARCHAR DEFAULT 'none',
            active BOOLEAN DEFAULT true,
            description VARCHAR,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR
        );
    """)
    existing_mappings = set(
        r[0] for r in con.execute("SELECT mapping_id FROM governance.source_field_mappings").fetchall()
    )
    for row in DEFAULT_SOURCE_FIELD_MAPPINGS + DEFAULT_PRODUCT_SOURCE_FIELD_MAPPINGS:
        if row[0] in existing_mappings:
            continue
        con.execute("""
            INSERT INTO governance.source_field_mappings
                (mapping_id, domain_key, source_system_key, attribute_key, source_expr,
                 transform_function, description)
            VALUES (?, ?, ?, ?, ?, ?, ?)
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
    entirely).

    No-ops (returns 0) if auth.users doesn't exist yet -- on a genuinely
    fresh clone, scripts/build_pipeline.py can run before the FastAPI app
    has ever started (which is what actually creates auth.users, via
    api/db.py's _ensure_auth_tables). api/db.py's own mirror of this
    function never hits this case, since get_con() always calls
    _ensure_auth_tables first."""
    auth_users_exists = con.execute("""
        SELECT count(*) FROM information_schema.tables
        WHERE table_schema = 'auth' AND table_name = 'users'
    """).fetchone()[0]
    if not auth_users_exists:
        return 0
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


def migrate_product_domain_access(con):
    """One-time, idempotent backfill for Product domain onboarding: every user
    who already has a governance.user_domain_access row for 'customer' gets an
    equivalent row for 'product' (same gold_access level), so flipping
    domain_registry.pipeline_status to 'live' doesn't silently lock everyone
    out of a domain nobody has an explicit grant for yet (see api/auth.py's
    domain_access_level() -- a user with no row for a domain has 'none', even
    if their legacy gold_access is 'read_write'). A governance user can then
    tune Product access per-user afterward via User Administration; this only
    seeds a reasonable starting point. Safe to re-run: a (user_id, 'product')
    pair that already has a row (including one a governance user has since
    changed) is left untouched.

    No-ops (returns 0) if auth.users doesn't exist yet, same reasoning as
    migrate_user_domain_access() above."""
    auth_users_exists = con.execute("""
        SELECT count(*) FROM information_schema.tables
        WHERE table_schema = 'auth' AND table_name = 'users'
    """).fetchone()[0]
    if not auth_users_exists:
        return 0
    customer_access = con.execute(
        "SELECT user_id, gold_access FROM governance.user_domain_access WHERE domain_key = 'customer'"
    ).fetchall()
    existing = set(
        r[0] for r in con.execute(
            "SELECT user_id FROM governance.user_domain_access WHERE domain_key = 'product'"
        ).fetchall()
    )
    migrated = 0
    for user_id, gold_access in customer_access:
        if user_id in existing:
            continue
        con.execute("""
            INSERT INTO governance.user_domain_access (user_id, domain_key, gold_access)
            VALUES (?, 'product', ?)
        """, (user_id, gold_access or "none"))
        migrated += 1
    return migrated


def main():
    con = duckdb.connect(DB_PATH)
    ensure_domain_tables(con)
    migrated = migrate_user_domain_access(con)
    migrated_product = migrate_product_domain_access(con)
    # Flush this run's ALTER TABLE ... ADD COLUMN ... DEFAULT statements out
    # of the WAL immediately rather than leaving them for whatever opens this
    # file next to replay -- see api/db.py's get_con() for why (this
    # installed DuckDB build can fail to replay such a WAL entry).
    con.execute("CHECKPOINT")
    print(
        "governance.domain_registry/domain_attributes/domain_source_systems/"
        "domain_relationships/user_domain_access ready. "
        f"{migrated} user(s) newly backfilled into user_domain_access (customer), "
        f"{migrated_product} user(s) newly backfilled into user_domain_access (product)."
    )


if __name__ == "__main__":
    main()
