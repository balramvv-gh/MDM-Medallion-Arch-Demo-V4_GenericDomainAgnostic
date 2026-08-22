"""
Provisions the 'ref' and 'bus_rules' schemas/tables (Reference Data
Maintenance and Rules Configuration, both under the Data Governance nav),
plus 'governance.orchestration_schedule'/'governance.orchestration_run_log'
(the MCP Orchestration Agent's metadata-driven schedule and run history --
see agents/orchestration_agent.py and MCP_Agent_Orchestration_Design.docx),
and seeds their starter rows if empty.

Why this script exists, standalone, separate from the FastAPI app: dbt's
silver models (stg_crm_customers.sql, stg_erp_customers.sql) now read
ref.ref_state_codes/ref_country_codes as a source, and the gold layer
(gold_crosswalk.sql) reads bus_rules.matching_thresholds as a source -- both
via {{ source(...) }}, not {{ ref(...) }}, since these are DB-native tables,
not dbt seeds. That means `dbt seed`/`dbt run` need these schemas to already
exist and be populated *before* dbt runs at all -- which can be before the
FastAPI app (api/db.py's _ensure_reference_tables/_ensure_bus_rules_tables)
has ever started, e.g. on a fresh clone. scripts/build_pipeline.py therefore
runs this script first, as step 0.

Same "CREATE TABLE IF NOT EXISTS + seed only if empty" DDL as
api/db.py's _ensure_reference_tables/_ensure_bus_rules_tables, duplicated here
deliberately -- this project's existing convention for schemas that both a
standalone script and the API might create (see api/db.py's
_ensure_match_review_tables docstring: "whichever of this script or the API
runs first creates them, both stay consistent"). If you change one, change
the other to match.
"""
import json
from pathlib import Path

import duckdb

DB_PATH = str(Path(__file__).resolve().parent.parent / "mdm_demo.duckdb")


DEFAULT_COUNTRY_CODES = [
    ("US", "United States"), ("CA", "Canada"), ("MX", "Mexico"), ("GB", "United Kingdom"),
    ("DE", "Germany"), ("FR", "France"), ("IN", "India"), ("CN", "China"),
    ("JP", "Japan"), ("AU", "Australia"),
]

DEFAULT_STATE_CODES = [
    ("AL", "Alabama"), ("AK", "Alaska"), ("AZ", "Arizona"), ("AR", "Arkansas"),
    ("CA", "California"), ("CO", "Colorado"), ("CT", "Connecticut"), ("DE", "Delaware"),
    ("FL", "Florida"), ("GA", "Georgia"), ("HI", "Hawaii"), ("ID", "Idaho"),
    ("IL", "Illinois"), ("IN", "Indiana"), ("IA", "Iowa"), ("KS", "Kansas"),
    ("KY", "Kentucky"), ("LA", "Louisiana"), ("ME", "Maine"), ("MD", "Maryland"),
    ("MA", "Massachusetts"), ("MI", "Michigan"), ("MN", "Minnesota"), ("MS", "Mississippi"),
    ("MO", "Missouri"), ("MT", "Montana"), ("NE", "Nebraska"), ("NV", "Nevada"),
    ("NH", "New Hampshire"), ("NJ", "New Jersey"), ("NM", "New Mexico"), ("NY", "New York"),
    ("NC", "North Carolina"), ("ND", "North Dakota"), ("OH", "Ohio"), ("OK", "Oklahoma"),
    ("OR", "Oregon"), ("PA", "Pennsylvania"), ("RI", "Rhode Island"), ("SC", "South Carolina"),
    ("SD", "South Dakota"), ("TN", "Tennessee"), ("TX", "Texas"), ("UT", "Utah"),
    ("VT", "Vermont"), ("VA", "Virginia"), ("WA", "Washington"), ("WV", "West Virginia"),
    ("WI", "Wisconsin"), ("WY", "Wyoming"), ("DC", "District of Columbia"),
]

DEFAULT_COLUMN_RULES = [
    ("R001", "CRM", "first_name", "not_null", None, "reject", "First name is required"),
    ("R002", "CRM", "last_name", "not_null", None, "reject", "Last name is required"),
    ("R003", "CRM", "email", "regex", r"^[^@\s]+@[^@\s]+\.[^@\s]+$", "reject", "Email must match standard format"),
    ("R004", "CRM", "state", "reference_state", None, "reject", "State must be a valid two-letter US state code"),
    ("R005", "CRM", "country", "reference_country", None, "reject", "Country must be a valid ISO country code"),
    ("R006", "CRM", "first_name", "trim_case_proper", None, "correct", "Trim whitespace and apply proper case"),
    ("R007", "CRM", "last_name", "trim_case_proper", None, "correct", "Trim whitespace and apply proper case"),
    ("R008", "CRM", "phone", "standardize_phone_us", None, "correct", "Standardize to E.164-style +1 format"),
    ("R009", "ERP", "full_name", "not_null", None, "reject", "Full name is required"),
    ("R010", "ERP", "email_addr", "regex", r"^[^@\s]+@[^@\s]+\.[^@\s]+$", "reject", "Email must match standard format"),
    ("R011", "ERP", "contact_phone", "not_null", None, "reject", "Phone is required"),
    ("R012", "ERP", "state_code", "reference_state", None, "reject", "State must be a valid two-letter US state code"),
    ("R013", "ERP", "country_code", "reference_country", None, "reject", "Country must be a valid ISO country code"),
    ("R014", "ERP", "email_addr", "lowercase", None, "correct", "Normalize email to lowercase"),
    ("R015", "ERP", "contact_phone", "standardize_phone_us", None, "correct", "Standardize to E.164-style +1 format"),
]

DEFAULT_MATCHING_RULES = [
    ("MR001", "MT001", "exact_match_field", 1, "email", "normalize_email", "Normalized (trimmed, lowercased) email must match exactly"),
    ("MR002", "MT001", "exact_match_field", 2, "phone", "normalize_phone", "Normalized (digits-only) phone must match exactly"),
    ("MR003", "MT002", "similarity_text_field", 1, "first_name", "none", "Included in the concatenated text used for TF-IDF similarity scoring"),
    ("MR004", "MT002", "similarity_text_field", 2, "last_name", "none", "Included in the concatenated text used for TF-IDF similarity scoring"),
    ("MR005", "MT002", "similarity_text_field", 3, "address_line1", "none", "Included in the concatenated text used for TF-IDF similarity scoring"),
    ("MR006", "MT002", "similarity_text_field", 4, "address_line2", "none", "Included in the concatenated text used for TF-IDF similarity scoring (optional field, blank treated as empty string)"),
    ("MR007", "MT002", "similarity_text_field", 5, "city", "none", "Included in the concatenated text used for TF-IDF similarity scoring"),
    ("MR008", "MT002", "blocking_key", 1, "state_code", "none", "Candidate pairs are only compared within the same blocking key value, to avoid O(n^2) comparisons at scale. Deliberately excludes email/phone from tier 2 entirely -- those are exactly the fields tier 2 exists to catch disagreement on."),
]

DEFAULT_MATCHING_THRESHOLDS = [
    ("MT000", 0, "No-Match Baseline", "no_match_baseline", False, 0.50, None,
     "Not a real matching tier -- confidence assigned to a single-source golden record with no corroborating match from any tier (\"provisional\")"),
    ("MT001", 1, "Tier 1: Exact Match", "exact", True, 1.00, None,
     "Two records are the same customer if they share an exact match on any active exact_match_field rule (see matching_rules) after that field's transform_function is applied. Binary -- no review band; confidence is always this tier's auto_merge_threshold."),
    ("MT002", 2, "Tier 2: Fuzzy Similarity Match", "fuzzy_tfidf_cosine", True, 0.80, 0.35,
     "Candidate pairs within the same blocking_key value (see matching_rules rule_role=blocking_key) are scored by TF-IDF character n-gram cosine similarity over the concatenated similarity_text_field columns. score >= auto_merge_threshold auto-merges; review_lower_threshold <= score < auto_merge_threshold is queued to the Match Review queue for steward confirm/reject; score < review_lower_threshold is not a candidate at all."),
]

DEFAULT_SURVIVORSHIP_RULES = [
    ("SR001", "first_name", "newest", None, "Most recently modified source wins"),
    ("SR002", "last_name", "newest", None, "Most recently modified source wins"),
    ("SR003", "email", "newest", None, "Most recently modified source wins"),
    ("SR004", "phone", "newest", None, "Most recently modified source wins"),
    ("SR005", "address_line1", "newest", None, "Most recently modified source wins"),
    ("SR006", "address_line2", "newest", None, "Most recently modified source wins"),
    ("SR007", "city", "newest", None, "Most recently modified source wins"),
    ("SR008", "state_code", "newest", None, "Most recently modified source wins"),
    ("SR009", "postal_code", "newest", None, "Most recently modified source wins"),
    ("SR010", "country_code", "newest", None, "Most recently modified source wins"),
]


# ---------------------------------------------------------------------------
# Product domain onboarding: bus_rules seed data for domain_key='product'.
# Unlike DEFAULT_COLUMN_RULES/MATCHING_RULES/MATCHING_THRESHOLDS/
# SURVIVORSHIP_RULES above (Customer's original rows, inserted without a
# domain_key -- domain_key is backfilled afterward by
# migrate_column_rules_to_domain.py / migrate_matching_rules_to_domain.py),
# these Product rows carry domain_key/attribute_key directly at insert time,
# since Product is being onboarded after those two migration scripts already
# exist -- there is no "pre-domain-key" era to replay for a brand-new domain.
#
# Seeded by ensure_product_bus_rules() below via PER-RULE_ID existence
# checks, NOT the whole-table-empty gate ensure_bus_rules_tables() uses above
# -- a live database already has Customer rows in every one of these four
# tables, so a whole-table-empty check would never fire again and Product's
# rows would silently never be inserted. See that function's docstring.
# ---------------------------------------------------------------------------

# Column rules (validation): 5 rules per source system, covering the
# attributes most load-bearing for matching (manufacturer_part_number,
# product_name) and a few of each source's own asymmetric attributes.
# Mirrors DEFAULT_COLUMN_RULES' shape/rule_type vocabulary exactly -- no new
# rule_type introduced here, only new rows (not_null/regex/reference_country
# reject a bad record; trim_case_proper corrects one in place, same as
# Customer's R006/R007).
DEFAULT_PRODUCT_COLUMN_RULES = [
    # rule_id, domain_key, attribute_key, source_system, source_column, rule_type, rule_param, severity, description
    ("R100", "product", "product_name", "ERP", "item_description", "not_null", None, "reject", "Product name is required"),
    ("R101", "product", "manufacturer_part_number", "ERP", "mfr_part_no", "not_null", None, "reject", "Manufacturer part number is required"),
    ("R102", "product", "list_price", "ERP", "unit_price", "regex", r"^\d+(\.\d{1,2})?$", "reject", "List price must be a positive number with up to 2 decimal places"),
    ("R103", "product", "supplier_id", "ERP", "vendor_id", "not_null", None, "reject", "Supplier ID is required"),
    ("R104", "product", "product_status", "ERP", "item_status", "trim_case_proper", None, "correct", "Trim whitespace and apply proper case"),
    ("R105", "product", "product_name", "PLM", "part_name", "not_null", None, "reject", "Product name is required"),
    ("R106", "product", "manufacturer_part_number", "PLM", "mpn", "not_null", None, "reject", "Manufacturer part number is required"),
    ("R107", "product", "weight_kg", "PLM", "weight_kg", "regex", r"^\d+(\.\d+)?$", "reject", "Weight must be a positive numeric value"),
    ("R108", "product", "country_of_origin", "PLM", "origin_country", "reference_country", None, "reject", "Country of origin must be a valid ISO country code"),
    ("R109", "product", "manufacturer_name", "PLM", "mfr_name", "trim_case_proper", None, "correct", "Trim whitespace and apply proper case"),
    ("R110", "product", "product_name", "SUPPLIER", "product_title", "not_null", None, "reject", "Product name is required"),
    ("R111", "product", "supplier_id", "SUPPLIER", "vendor_account_id", "not_null", None, "reject", "Supplier account ID is required"),
    ("R112", "product", "lead_time_days", "SUPPLIER", "lead_time", "regex", r"^\d+$", "reject", "Lead time must be a whole number of days"),
    ("R113", "product", "minimum_order_qty", "SUPPLIER", "moq", "regex", r"^\d+$", "reject", "Minimum order quantity must be a whole number"),
    ("R114", "product", "country_of_origin", "SUPPLIER", "country_of_mfg", "reference_country", None, "reject", "Country of origin must be a valid ISO country code"),
]

# Matching rules: manufacturer_part_number is the sole tier-1 exact key
# (normalized via the new 'normalize_code' transform_function -- strips
# non-alphanumerics and upper-cases, so "MPN-4471-A" and "mpn4471a" compare
# equal); product_name/manufacturer_name feed tier-2's TF-IDF similarity
# text; category is tier-2's blocking key (mirrors Customer's MR001-MR008
# shape exactly, just scoped to domain_key='product' and Product's own
# attribute names).
DEFAULT_PRODUCT_MATCHING_RULES = [
    # rule_id, domain_key, attribute_key, tier_id, rule_role, rule_order, source_column, transform_function, description
    ("MR100", "product", "manufacturer_part_number", "MT101", "exact_match_field", 1, "manufacturer_part_number", "normalize_code",
     "Normalized (uppercased, alphanumeric-only) manufacturer part number must match exactly"),
    ("MR101", "product", "product_name", "MT102", "similarity_text_field", 1, "product_name", "none",
     "Included in the concatenated text used for TF-IDF similarity scoring"),
    ("MR102", "product", "manufacturer_name", "MT102", "similarity_text_field", 2, "manufacturer_name", "none",
     "Included in the concatenated text used for TF-IDF similarity scoring"),
    ("MR103", "product", "category", "MT102", "blocking_key", 1, "category", "none",
     "Candidate pairs are only compared within the same blocking key value, to avoid O(n^2) comparisons at scale."),
]

DEFAULT_PRODUCT_MATCHING_THRESHOLDS = [
    # tier_id, domain_key, tier_order, tier_name, match_method, is_match_tier, auto_merge_threshold, review_lower_threshold, description
    ("MT100", "product", 0, "No-Match Baseline", "no_match_baseline", False, 0.50, None,
     "Not a real matching tier -- confidence assigned to a single-source golden record with no corroborating match from any tier (\"provisional\")"),
    ("MT101", "product", 1, "Tier 1: Exact Match", "exact", True, 1.00, None,
     "Two products are the same if they share an exact match on manufacturer_part_number (see matching_rules MR100) after normalize_code is applied. Binary -- confidence is always this tier's auto_merge_threshold."),
    ("MT102", "product", 2, "Tier 2: Fuzzy Similarity Match", "fuzzy_tfidf_cosine", True, 0.80, 0.60,
     "Candidate pairs within the same category (blocking_key, MR103) are scored by TF-IDF character n-gram cosine similarity over the concatenated product_name/manufacturer_name text. score >= auto_merge_threshold auto-merges; review_lower_threshold <= score < auto_merge_threshold is queued to the Match Review queue; score < review_lower_threshold is not a candidate at all. review_lower_threshold is higher than Customer's 0.35 (MT002) deliberately: with only a handful of distinct product component types per category, unrelated products sharing a category and component word can otherwise cross a lower bar purely on that shared vocabulary -- 0.60 was calibrated against this domain's own generated data (see data/generate_product_source_data.py) to keep the Match Review queue to genuinely plausible candidates."),
]

# One rule per Product attribute that participates in survivorship (the 18
# non-structural attributes -- source_created_date/source_modified_date, like
# Customer's, have no survivorship rule and fall back to the universal
# tie-break). Deliberately varied across every rule_type this project
# supports, including the new 'source_priority' ("select if source is X")
# the user asked for by name, so the demo shows every rule_type in one place:
#   newest          -- product_name
#   oldest          -- category
#   pattern_match   -- engineering_spec_version (prefers the well-formed "REV-n.n" shape)
#   shortest_value  -- color (prefers the more concise value over verbose marketing text)
#   most_common     -- unit_of_measure, country_of_origin
#   most_complete   -- manufacturer_name, weight_kg, material, supplier_id, supplier_part_number
#   source_priority -- manufacturer_part_number/lifecycle_status (PLM); list_price/
#                      currency_code/product_status (ERP); lead_time_days/
#                      minimum_order_qty (SUPPLIER)
#
# Rule-type choice per attribute is NOT arbitrary -- it depends on which of
# ERP/PLM/SUPPLIER actually map that attribute (see DEFAULT_PRODUCT_SOURCE_FIELD_MAPPINGS
# above), because gold_survivorship_winners.sql's per-attribute unpivot emits
# a row (with a NULL value) for EVERY contributing source record, even one
# whose source_field_mappings has no row at all for that attribute. 'newest'/
# 'oldest' rank purely by date, blind to whether the value itself is
# populated -- safe ONLY for product_name/manufacturer_part_number/category,
# the three attributes every one of ERP/PLM/SUPPLIER maps, where that NULL
# case can never arise. For every other (asymmetrically-mapped) attribute,
# this uses 'most_complete' (prefers non-blank, safe regardless of which
# source combination happens to make up a match group) or 'source_priority'
# naming the attribute's ONE sole mapping source (also always safe: the sole
# mapper is the only row that can ever score a match on source identity, so
# it wins outright whether or not any other, non-mapping source's row in the
# same group happens to be null). 'most_common'/'shortest_value' are safe for
# any attribute already, since both explicitly exclude a blank/null value
# from ever "winning" (see this file's SQL for the exact scoring logic).
DEFAULT_PRODUCT_SURVIVORSHIP_RULES = [
    # rule_id, domain_key, attribute_key, target_column, rule_type, rule_param, description
    ("SR100", "product", "product_name", "product_name", "newest", None,
     "Most recently modified source wins (safe here: ERP/PLM/Supplier all map product_name)"),
    ("SR101", "product", "manufacturer_part_number", "manufacturer_part_number", "source_priority", "PLM",
     "PLM (engineering) is this demo's system of record for the manufacturer part number"),
    ("SR102", "product", "manufacturer_name", "manufacturer_name", "most_complete", None,
     "Prefers a populated manufacturer name over a blank one -- ERP does not carry this attribute at all"),
    ("SR103", "product", "category", "category", "oldest", None,
     "Category classification is set at initial product creation and rarely reclassified afterward -- "
     "the earliest recorded value is treated as authoritative (safe here: ERP/PLM/Supplier all map category)"),
    ("SR104", "product", "unit_of_measure", "unit_of_measure", "most_common", None,
     "The non-blank value that appears in the most contributing source records wins"),
    ("SR105", "product", "list_price", "list_price", "source_priority", "ERP",
     "ERP is this demo's sole source of commercial pricing"),
    ("SR106", "product", "currency_code", "currency_code", "source_priority", "ERP",
     "ERP is this demo's sole source of commercial pricing"),
    ("SR107", "product", "product_status", "product_status", "source_priority", "ERP",
     "ERP's commercial lifecycle status is authoritative"),
    ("SR108", "product", "lifecycle_status", "lifecycle_status", "source_priority", "PLM",
     "PLM's engineering lifecycle status is authoritative"),
    ("SR109", "product", "weight_kg", "weight_kg", "most_complete", None,
     "Prefers a populated weight over a blank one -- ERP's free-text gross_weight can fail numeric_extract, "
     "and Supplier does not carry this attribute at all"),
    ("SR110", "product", "material", "material", "most_complete", None,
     "Prefers a populated material spec over a blank one -- ERP does not carry this attribute at all"),
    ("SR111", "product", "color", "color", "shortest_value", None,
     "Prefers the more concise value across sources over verbose supplier marketing text"),
    ("SR112", "product", "country_of_origin", "country_of_origin", "most_common", None,
     "The non-blank value that appears in the most contributing source records wins"),
    ("SR113", "product", "supplier_id", "supplier_id", "most_complete", None,
     "Prefers a populated supplier ID over a blank one -- PLM does not carry this attribute at all"),
    ("SR114", "product", "supplier_part_number", "supplier_part_number", "most_complete", None,
     "Prefers a populated supplier part number over a blank one -- PLM does not carry this attribute at all"),
    ("SR115", "product", "lead_time_days", "lead_time_days", "source_priority", "SUPPLIER",
     "Only Supplier carries this attribute -- always defers to Supplier's own reported value"),
    ("SR116", "product", "minimum_order_qty", "minimum_order_qty", "source_priority", "SUPPLIER",
     "Only Supplier carries this attribute -- always defers to Supplier's own reported value"),
    ("SR117", "product", "engineering_spec_version", "engineering_spec_version", "pattern_match", r"^REV-\d+(\.\d+)?$",
     "Prefers a value matching the standard \"REV-<major>[.<minor>]\" format over a malformed one "
     "(safe in practice: only PLM carries this attribute, and its generated values always match)"),
]


def ensure_product_bus_rules(con):
    """Seeds Product's bus_rules rows (DEFAULT_PRODUCT_COLUMN_RULES/
    MATCHING_RULES/MATCHING_THRESHOLDS/SURVIVORSHIP_RULES) into an
    already-provisioned bus_rules schema, using a PER-RULE_ID existence
    check -- unlike ensure_bus_rules_tables() above, which only seeds
    DEFAULT_COLUMN_RULES/etc. the first time (a whole-table-empty check).
    On a live database that already has Customer's rows in every one of
    these four tables, that whole-table-empty check will never fire again,
    so simply appending Product's rows to those same DEFAULT_* lists would
    silently never insert them. This function is the fix: it runs every
    time (idempotent, like every other *_bus_rules helper in this file) and
    inserts only the rule_ids that don't already exist yet.

    Called from main(), after ensure_bus_rules_tables() so the tables
    already exist. Must be mirrored in api/db.py's _ensure_product_bus_rules
    -- see that file's module docstring for the "kept in sync" convention."""
    existing_column_rules = set(r[0] for r in con.execute("SELECT rule_id FROM bus_rules.column_rules").fetchall())
    for rule_id, domain_key, attribute_key, source_system, source_column, rule_type, rule_param, severity, description in DEFAULT_PRODUCT_COLUMN_RULES:
        if rule_id in existing_column_rules:
            continue
        con.execute("""
            INSERT INTO bus_rules.column_rules
                (rule_id, domain_key, attribute_key, source_system, source_column, rule_type, rule_param, severity, description)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (rule_id, domain_key, attribute_key, source_system, source_column, rule_type, rule_param, severity, description))

    existing_matching_rules = set(r[0] for r in con.execute("SELECT rule_id FROM bus_rules.matching_rules").fetchall())
    for rule_id, domain_key, attribute_key, tier_id, rule_role, rule_order, source_column, transform_function, description in DEFAULT_PRODUCT_MATCHING_RULES:
        if rule_id in existing_matching_rules:
            continue
        con.execute("""
            INSERT INTO bus_rules.matching_rules
                (rule_id, domain_key, attribute_key, tier_id, rule_role, rule_order, source_column, transform_function, description)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (rule_id, domain_key, attribute_key, tier_id, rule_role, rule_order, source_column, transform_function, description))

    existing_thresholds = set(r[0] for r in con.execute("SELECT tier_id FROM bus_rules.matching_thresholds").fetchall())
    for tier_id, domain_key, tier_order, tier_name, match_method, is_match_tier, auto_merge_threshold, review_lower_threshold, description in DEFAULT_PRODUCT_MATCHING_THRESHOLDS:
        if tier_id in existing_thresholds:
            continue
        con.execute("""
            INSERT INTO bus_rules.matching_thresholds
                (tier_id, domain_key, tier_order, tier_name, match_method, is_match_tier,
                 auto_merge_threshold, review_lower_threshold, description)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (tier_id, domain_key, tier_order, tier_name, match_method, is_match_tier, auto_merge_threshold, review_lower_threshold, description))

    existing_survivorship = set(r[0] for r in con.execute("SELECT rule_id FROM bus_rules.survivorship_rules").fetchall())
    for rule_id, domain_key, attribute_key, target_column, rule_type, rule_param, description in DEFAULT_PRODUCT_SURVIVORSHIP_RULES:
        if rule_id in existing_survivorship:
            continue
        con.execute("""
            INSERT INTO bus_rules.survivorship_rules
                (rule_id, domain_key, attribute_key, target_column, rule_type, rule_param, description)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (rule_id, domain_key, attribute_key, target_column, rule_type, rule_param, description))


def ensure_reference_tables(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS ref;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS ref.ref_country_codes (
            country_code VARCHAR PRIMARY KEY,
            country_name VARCHAR,
            is_active BOOLEAN DEFAULT true,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ref.ref_state_codes (
            state_code VARCHAR PRIMARY KEY,
            state_name VARCHAR,
            is_active BOOLEAN DEFAULT true,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR
        );
    """)
    if con.execute("SELECT COUNT(*) FROM ref.ref_country_codes").fetchone()[0] == 0:
        for row in DEFAULT_COUNTRY_CODES:
            con.execute("INSERT INTO ref.ref_country_codes (country_code, country_name) VALUES (?, ?)", row)
    if con.execute("SELECT COUNT(*) FROM ref.ref_state_codes").fetchone()[0] == 0:
        for row in DEFAULT_STATE_CODES:
            con.execute("INSERT INTO ref.ref_state_codes (state_code, state_name) VALUES (?, ?)", row)


def ensure_bus_rules_tables(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS bus_rules;")
    # Phase 2/3 (domain-agnostic) parity fix: domain_key/attribute_key are now
    # baked directly into CREATE TABLE here, matching api/db.py's
    # _ensure_bus_rules_tables -- this script's copy had drifted and lacked
    # them entirely (they were only ever added via ALTER TABLE ADD COLUMN by
    # scripts/migrate_column_rules_to_domain.py / migrate_matching_rules_to_domain.py,
    # which run AFTER this script in scripts/build_pipeline.py's step order).
    # Baking them in here means a brand-new database gets the correct shape
    # immediately -- needed so ensure_product_bus_rules() below (called later
    # in this same script's main()) can insert domain_key='product' rows
    # without waiting on those two migration scripts to run first. Harmless
    # for an existing table: CREATE TABLE IF NOT EXISTS is a no-op there, and
    # the migration scripts' idempotent ALTER TABLE ADD COLUMN IF NOT EXISTS
    # already carry that shape forward regardless.
    con.execute("""
        CREATE TABLE IF NOT EXISTS bus_rules.column_rules (
            rule_id VARCHAR PRIMARY KEY,
            domain_key VARCHAR,
            attribute_key VARCHAR,
            source_system VARCHAR,
            source_column VARCHAR,
            rule_type VARCHAR,
            rule_param VARCHAR,
            severity VARCHAR,
            active BOOLEAN DEFAULT true,
            description VARCHAR,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bus_rules.matching_rules (
            rule_id VARCHAR PRIMARY KEY,
            domain_key VARCHAR,
            attribute_key VARCHAR,
            tier_id VARCHAR,
            rule_role VARCHAR,
            rule_order INTEGER,
            source_column VARCHAR,
            transform_function VARCHAR,
            active BOOLEAN DEFAULT true,
            description VARCHAR,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bus_rules.matching_thresholds (
            tier_id VARCHAR PRIMARY KEY,
            domain_key VARCHAR,
            tier_order INTEGER,
            tier_name VARCHAR,
            match_method VARCHAR,
            is_match_tier BOOLEAN,
            auto_merge_threshold DOUBLE,
            review_lower_threshold DOUBLE,
            active BOOLEAN DEFAULT true,
            description VARCHAR,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bus_rules.survivorship_rules (
            rule_id VARCHAR PRIMARY KEY,
            domain_key VARCHAR,
            attribute_key VARCHAR,
            target_column VARCHAR,
            rule_type VARCHAR,
            rule_param VARCHAR,
            active BOOLEAN DEFAULT true,
            description VARCHAR,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR
        );
    """)

    if con.execute("SELECT COUNT(*) FROM bus_rules.column_rules").fetchone()[0] == 0:
        for row in DEFAULT_COLUMN_RULES:
            con.execute("""
                INSERT INTO bus_rules.column_rules
                    (rule_id, source_system, source_column, rule_type, rule_param, severity, description)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, row)
    if con.execute("SELECT COUNT(*) FROM bus_rules.matching_rules").fetchone()[0] == 0:
        for row in DEFAULT_MATCHING_RULES:
            con.execute("""
                INSERT INTO bus_rules.matching_rules
                    (rule_id, tier_id, rule_role, rule_order, source_column, transform_function, description)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, row)
    if con.execute("SELECT COUNT(*) FROM bus_rules.matching_thresholds").fetchone()[0] == 0:
        for row in DEFAULT_MATCHING_THRESHOLDS:
            con.execute("""
                INSERT INTO bus_rules.matching_thresholds
                    (tier_id, tier_order, tier_name, match_method, is_match_tier,
                     auto_merge_threshold, review_lower_threshold, description)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, row)
    if con.execute("SELECT COUNT(*) FROM bus_rules.survivorship_rules").fetchone()[0] == 0:
        for row in DEFAULT_SURVIVORSHIP_RULES:
            con.execute("""
                INSERT INTO bus_rules.survivorship_rules (rule_id, target_column, rule_type, rule_param, description)
                VALUES (?, ?, ?, ?, ?)
            """, row)


# ---------------------------------------------------------------------------
# Phase 1 (flexibility): governance.entity_type_registry -- a single
# metadata-driven catalog describing every Reference Data Maintenance and
# Rules Configuration entity type, replacing the six places that previously
# had to be edited by hand to add a new one (REFERENCE_ENTITY_CONFIG /
# RULES_ENTITY_CONFIG in api/main.py, this file's/api/db.py's DDL, the portal's
# REF_DATA_TABS / RULES_TABS / REFDATA_FIELD_LABELS / RULES_FIELD_LABELS JS
# objects duplicated across portal_app and stewardship_app). api/main.py's
# GET /api/v1/governed-entities serves this table to both frontends, which
# render tabs, table columns, and create/edit forms entirely from it.
#
# field_schema is a JSON array of {key, label, type, options?, role?}:
#   type   -- 'text' | 'number' | 'select' | 'boolean'; drives the form input
#             rendered and how the value is coerced before submit.
#   options -- required when type='select'; also doubles as the
#             submission-time allow-list for controlled-vocabulary fields
#             (api/main.py's _validate_rules_config_values, Phase 0) once a
#             field's options here are wired in as its source of truth.
#   role   -- reference_data entities only: 'pk' marks the user-entered code
#             field, 'name' marks its human-readable label field. Absent for
#             rules_config fields (those use pk_column instead, since the
#             primary key there is a system-generated id, not user-entered).
# is_active is always the final field_schema entry -- every governed entity
# uses the same "deactivate, never hard-delete" convention.
#
# This table only drives the admin/CRUD/approval-diff *rendering* layer --
# it does not make a new rule_type/match_method executable. That's Phase 2
# (RULE_TYPE_HANDLERS-style registries in api/validation.py, the dbt models,
# and scripts/generate_matches.py). Adding a row here for a genuinely new
# governed table (not just a new *value* within an existing one) also still
# requires creating that physical table once, same as any typed schema
# change -- see this file's docstring for why DB-native tables, not a
# generic JSON blob table, were chosen (typed, dbt-inspectable columns).
# ---------------------------------------------------------------------------

DEFAULT_ENTITY_TYPE_REGISTRY = [
    # (entity_type, category, display_label, display_label_singular, table_name,
    #  pk_column, order_column, id_prefix, workflow_type, name_field, sort_order, field_schema)
    (
        "ref_country_code", "reference_data", "Country Codes", "Country Code",
        "ref.ref_country_codes", "country_code", "country_code", None, "reference_data_change",
        "country_name", 1,
        json.dumps([
            {"key": "country_code", "label": "Country Code", "type": "text", "role": "pk"},
            {"key": "country_name", "label": "Country Name", "type": "text", "role": "name"},
            {"key": "is_active", "label": "Status", "type": "boolean"},
        ]),
    ),
    (
        "ref_state_code", "reference_data", "State Codes", "State Code",
        "ref.ref_state_codes", "state_code", "state_code", None, "reference_data_change",
        "state_name", 2,
        json.dumps([
            {"key": "state_code", "label": "State Code", "type": "text", "role": "pk"},
            {"key": "state_name", "label": "State Name", "type": "text", "role": "name"},
            {"key": "is_active", "label": "Status", "type": "boolean"},
        ]),
    ),
    # Phase 4 (domain-agnostic) parity fix: every rules_config field_schema
    # below now adds domain_key/attribute_key as plain text fields, instead
    # of a hardcoded 'select' of literal Customer domain/attribute names
    # (source_system used to be a CRM/ERP-only select; target_column used to
    # be a select hardcoding Customer's 10 attribute names) -- this brings
    # this script's copy up to parity with api/db.py's DEFAULT_ENTITY_TYPE_REGISTRY,
    # which already generalized this. A steward types the domain_key/
    # attribute_key value directly; GET /api/v1/domains and
    # GET /api/v1/rules-config/... already expose the live governed lists for
    # a frontend to render as an actual dropdown without any hardcoding here.
    (
        "column_rule", "rules_config", "Column Rules", "Column Rule",
        "bus_rules.column_rules", "rule_id", "rule_id", "CR", "rules_config_change",
        None, 1,
        json.dumps([
            {"key": "domain_key", "label": "Domain", "type": "text"},
            {"key": "attribute_key", "label": "Attribute", "type": "text"},
            {"key": "source_system", "label": "Source System", "type": "text"},
            {"key": "source_column", "label": "Source Column (legacy, reference only)", "type": "text"},
            {"key": "rule_type", "label": "Rule Type", "type": "select", "options": [
                "not_null", "regex", "reference_state", "reference_country",
                "trim_case_proper", "standardize_phone_us", "lowercase",
            ]},
            {"key": "rule_param", "label": "Rule Param", "type": "text"},
            {"key": "severity", "label": "Severity", "type": "select", "options": ["reject", "correct"]},
            {"key": "description", "label": "Description", "type": "text"},
            {"key": "is_active", "label": "Status", "type": "boolean"},
        ]),
    ),
    (
        "matching_rule", "rules_config", "Matching Rules", "Matching Rule",
        "bus_rules.matching_rules", "rule_id", "rule_id", "MR", "rules_config_change",
        None, 2,
        json.dumps([
            {"key": "domain_key", "label": "Domain", "type": "text"},
            {"key": "attribute_key", "label": "Attribute", "type": "text"},
            {"key": "tier_id", "label": "Tier ID", "type": "text"},
            {"key": "rule_role", "label": "Rule Role", "type": "select",
             "options": ["exact_match_field", "similarity_text_field", "blocking_key"]},
            {"key": "rule_order", "label": "Rule Order", "type": "number"},
            {"key": "source_column", "label": "Source Column (legacy, reference only)", "type": "text"},
            {"key": "transform_function", "label": "Transform Function", "type": "select",
             "options": ["none", "normalize_email", "normalize_phone", "normalize_code"]},
            {"key": "description", "label": "Description", "type": "text"},
            {"key": "is_active", "label": "Status", "type": "boolean"},
        ]),
    ),
    (
        "matching_threshold", "rules_config", "Matching Tiers", "Matching Tier",
        "bus_rules.matching_thresholds", "tier_id", "tier_order", "MT", "rules_config_change",
        None, 3,
        json.dumps([
            {"key": "domain_key", "label": "Domain", "type": "text"},
            {"key": "tier_order", "label": "Tier Order", "type": "number"},
            {"key": "tier_name", "label": "Tier Name", "type": "text"},
            # Phase 0 upgraded match_method from unconstrained free text to a
            # select constrained to methods scripts/generate_matches.py
            # actually implements -- see RULES_FIELD_OPTIONS in api/main.py.
            {"key": "match_method", "label": "Match Method", "type": "select",
             "options": ["exact", "fuzzy_tfidf_cosine", "no_match_baseline"]},
            {"key": "is_match_tier", "label": "Is Match Tier", "type": "boolean"},
            {"key": "auto_merge_threshold", "label": "Auto-Merge Threshold", "type": "number"},
            {"key": "review_lower_threshold", "label": "Review Lower Threshold", "type": "number"},
            {"key": "description", "label": "Description", "type": "text"},
            {"key": "is_active", "label": "Status", "type": "boolean"},
        ]),
    ),
    (
        "survivorship_rule", "rules_config", "Survivorship Rules", "Survivorship Rule",
        "bus_rules.survivorship_rules", "rule_id", "rule_id", "SR", "rules_config_change",
        None, 4,
        json.dumps([
            {"key": "domain_key", "label": "Domain", "type": "text"},
            {"key": "attribute_key", "label": "Attribute", "type": "text"},
            {"key": "target_column", "label": "Target Column (legacy, reference only)", "type": "text"},
            {"key": "rule_type", "label": "Rule Type", "type": "select",
             "options": ["most_common", "most_complete", "oldest", "newest", "pattern_match", "shortest_value", "source_priority"]},
            {"key": "rule_param", "label": "Rule Param (pattern_match: a regex; source_priority: the preferred source_system_key)", "type": "text"},
            {"key": "description", "label": "Description", "type": "text"},
            {"key": "is_active", "label": "Status", "type": "boolean"},
        ]),
    ),
]


DEFAULT_ORCHESTRATION_SCHEDULE = [
    # schedule_id, pipeline_name, cron_expression, timezone, stage_sequence,
    # run_mode, on_failure, enabled, description
    ("SCHED001", "full_batch_pipeline", "0 2 * * *", "UTC",
     ["ingestion", "dq", "matching", "survivorship", "steward"],
     "auto", "halt", True,
     "Nightly full pipeline run (Ingestion -> DQ -> Matching -> Survivorship -> "
     "Steward hygiene) at 02:00 UTC, mirroring scripts/build_pipeline.py's stage "
     "order. run_mode='auto' defers full-vs-incremental detection to the "
     "Ingestion Agent (same bronze.load_batches-driven logic load_bronze.py/ "
     "build_pipeline.py already use)."),
]


def ensure_orchestration_tables(con):
    """Provisions governance.orchestration_schedule and
    governance.orchestration_run_log -- the metadata-driven schedule the
    Orchestration Agent (agents/orchestration_agent.py) reads instead of a
    hardcoded cron, and the append-only run history the Monitoring Agent
    reads for SLA/health checks. Same 'CREATE TABLE IF NOT EXISTS + seed only
    if empty' convention as ensure_bus_rules_tables above -- see this file's
    module docstring for why this script (not just api/db.py) provisions
    these schemas.

    orchestration_schedule mirrors bus_rules.column_rules' shape (rule-like
    PK, active flag, created_ts/updated_ts/updated_by) so it can eventually
    be exposed through the Rules Configuration screen via
    governance.entity_type_registry, the same way column_rules/matching_rules/
    matching_thresholds/survivorship_rules already are -- not wired up yet,
    tracked as follow-up work (see MCP_Agent_Orchestration_Design.docx).

    orchestration_run_log is append-only, one row per orchestration run,
    same convention as bronze.load_batches/audit.audit_trail -- never
    updated after being written, only inserted. stage_results is a JSON
    string (VARCHAR, not a nested type) so DuckDB's JSON functions and
    plain Python json.loads() both work without a struct schema that would
    have to change every time a stage is added or renamed.
    """
    con.execute("CREATE SCHEMA IF NOT EXISTS governance;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS governance.orchestration_schedule (
            schedule_id VARCHAR PRIMARY KEY,
            pipeline_name VARCHAR,
            cron_expression VARCHAR,       -- standard 5-field cron, evaluated in `timezone`
            timezone VARCHAR DEFAULT 'UTC',
            stage_sequence VARCHAR[],      -- ordered agent names, e.g. ['ingestion','dq','matching','survivorship','steward']
            run_mode VARCHAR DEFAULT 'auto',   -- 'auto' | 'full' | 'incremental' (passed to Ingestion Agent)
            on_failure VARCHAR DEFAULT 'halt', -- 'halt' | 'continue' | 'alert_only'
            active BOOLEAN DEFAULT true,
            description VARCHAR,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS governance.orchestration_run_log (
            run_id VARCHAR PRIMARY KEY,
            schedule_id VARCHAR,           -- NULL for an ad hoc/manual trigger
            pipeline_name VARCHAR,
            triggered_by VARCHAR,          -- 'scheduler' | 'manual:<username>'
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            status VARCHAR,                -- 'running' | 'success' | 'failed' | 'partial'
            stage_results VARCHAR          -- JSON array: [{stage, status, started_at, ended_at, row_counts, error}, ...]
        );
    """)

    if con.execute("SELECT COUNT(*) FROM governance.orchestration_schedule").fetchone()[0] == 0:
        for row in DEFAULT_ORCHESTRATION_SCHEDULE:
            con.execute("""
                INSERT INTO governance.orchestration_schedule
                    (schedule_id, pipeline_name, cron_expression, timezone, stage_sequence,
                     run_mode, on_failure, active, description)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row)


DEFAULT_MONITORING_THRESHOLDS = [
    # metric_name, warning_threshold, critical_threshold, unit, description
    ("pipeline_staleness_hours", 26, 48, "hours",
     "Hours since the last successful full_batch_pipeline run (governance.orchestration_run_log) "
     "before the Monitoring Agent reports degraded/critical. 26 gives a 2-hour buffer past the "
     "nightly 02:00 UTC schedule in governance.orchestration_schedule."),
    ("stage_duration_minutes", 15, 60, "minutes",
     "Any single agent stage's duration (AgentResult.duration_seconds) before flagging "
     "degraded/critical -- same threshold applied to all five pipeline stages today."),
    ("exceptions_open_count", 50, 200, "count",
     "Open DQ exception queue depth (main_silver.exceptions_queue joined with "
     "stewardship.exception_status_overrides) before flagging degraded/critical."),
    ("match_review_pending_count", 50, 200, "count",
     "Pending Match Review queue depth (main_gold.gold_match_review_queue joined with "
     "stewardship.match_review_overrides) before flagging degraded/critical."),
]


def ensure_monitoring_tables(con):
    """Provisions governance.monitoring_thresholds -- SLA/DQ/health
    thresholds for the Monitoring Agent (agents/monitoring_agent.py), kept
    DB-native and metadata-driven for the same reason
    governance.orchestration_schedule is: so alert sensitivity can be
    tuned without a code change, and so it can eventually be surfaced
    through Rules Configuration alongside the other rule tables. Same
    'CREATE TABLE IF NOT EXISTS + seed only if empty' convention as the
    rest of this file."""
    con.execute("CREATE SCHEMA IF NOT EXISTS governance;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS governance.monitoring_thresholds (
            metric_name VARCHAR PRIMARY KEY,
            warning_threshold DOUBLE,
            critical_threshold DOUBLE,
            unit VARCHAR,
            active BOOLEAN DEFAULT true,
            description VARCHAR,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR
        );
    """)
    if con.execute("SELECT COUNT(*) FROM governance.monitoring_thresholds").fetchone()[0] == 0:
        for row in DEFAULT_MONITORING_THRESHOLDS:
            con.execute("""
                INSERT INTO governance.monitoring_thresholds
                    (metric_name, warning_threshold, critical_threshold, unit, description)
                VALUES (?, ?, ?, ?, ?)
            """, row)


def ensure_entity_type_registry(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS governance;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS governance.entity_type_registry (
            entity_type VARCHAR PRIMARY KEY,
            category VARCHAR,              -- 'reference_data' | 'rules_config'
            display_label VARCHAR,         -- plural, tab button text
            display_label_singular VARCHAR,-- 'New X' title / Approvals record header
            table_name VARCHAR,            -- 'schema.table' this entity_type reads/writes
            pk_column VARCHAR,
            order_column VARCHAR,          -- defaults to pk_column if same
            id_prefix VARCHAR,             -- rules_config only; NULL for reference_data
            workflow_type VARCHAR,         -- 'reference_data_change' | 'rules_config_change'
            name_field VARCHAR,            -- reference_data only; NULL for rules_config
            sort_order INTEGER,            -- tab display order within category
            field_schema VARCHAR,          -- JSON array, see module docstring above
            active BOOLEAN DEFAULT true,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp
        );
    """)
    # Per-entity_type existence check (not a whole-table-empty check), same
    # convention as governance.workflow_definitions in api/db.py -- so this
    # table can be introduced into an already-running DB (this demo's normal
    # case) and backfilled without disturbing any custom entity_type rows
    # someone may have already added directly.
    existing = set(
        con.execute("SELECT entity_type FROM governance.entity_type_registry").fetchdf()["entity_type"]
    )
    for row in DEFAULT_ENTITY_TYPE_REGISTRY:
        if row[0] in existing:
            continue
        con.execute("""
            INSERT INTO governance.entity_type_registry
                (entity_type, category, display_label, display_label_singular, table_name,
                 pk_column, order_column, id_prefix, workflow_type, name_field, sort_order, field_schema)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row)

    # field_schema can drift from DEFAULT_ENTITY_TYPE_REGISTRY as this file
    # evolves (e.g. Product onboarding added domain_key/attribute_key fields
    # and new rule_type/transform_function/match_method options) -- this
    # table has no write endpoint anywhere in the API, so re-syncing every
    # row's field_schema to the current DEFAULT_ENTITY_TYPE_REGISTRY on every
    # run is always safe. Mirrors api/db.py's _ensure_entity_type_registry,
    # which already had this resync loop -- this script's copy did not,
    # until now.
    for row in DEFAULT_ENTITY_TYPE_REGISTRY:
        con.execute(
            "UPDATE governance.entity_type_registry SET field_schema = ? WHERE entity_type = ?",
            [row[-1], row[0]],
        )


def main():
    con = duckdb.connect(DB_PATH)
    ensure_reference_tables(con)
    ensure_bus_rules_tables(con)
    ensure_product_bus_rules(con)
    ensure_orchestration_tables(con)
    ensure_monitoring_tables(con)
    ensure_entity_type_registry(con)
    print("ref.*, bus_rules.*, and governance.orchestration_*/monitoring_* schemas/tables ready.")


if __name__ == "__main__":
    main()
