import duckdb
import json
import threading
from pathlib import Path

# Portable: resolves to <project_root>/mdm_demo.duckdb regardless of OS or
# where the project was checked out / extracted.
DB_PATH = str(Path(__file__).resolve().parent.parent / "mdm_demo.duckdb")

_lock = threading.Lock()
_con = None


def get_con():
    global _con
    if _con is None:
        _con = duckdb.connect(DB_PATH)
        _ensure_stewardship_tables(_con)
        _ensure_match_review_tables(_con)
        _ensure_silver_snapshot_table(_con)
        _ensure_auth_tables(_con)
        _ensure_audit_tables(_con)
        _ensure_governance_tables(_con)
        _ensure_reference_tables(_con)
        _ensure_bus_rules_tables(_con)
        _ensure_entity_type_registry(_con)
        _ensure_domain_tables(_con)
    return _con


def _ensure_auth_tables(con):
    """Creates the app-level auth tables (users + sessions) for the UX portal."""
    con.execute("CREATE SCHEMA IF NOT EXISTS auth;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS auth.users (
            user_id VARCHAR PRIMARY KEY,
            username VARCHAR UNIQUE,
            password_hash VARCHAR,
            full_name VARCHAR,
            role VARCHAR,           -- 'admin' | 'dataSteward' | 'dataOwner' | 'businessUser' -- drives app menu visibility
            gold_access VARCHAR,    -- 'read_write' | 'read' | 'none' -- drives gold-layer data permission
            is_active BOOLEAN DEFAULT true,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            last_login_ts TIMESTAMP
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS auth.sessions (
            token VARCHAR PRIMARY KEY,
            user_id VARCHAR,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            expires_ts TIMESTAMP
        );
    """)


def _ensure_stewardship_tables(con):
    """Creates the stewardship-side tables if this is a fresh DB file."""
    con.execute("CREATE SCHEMA IF NOT EXISTS stewardship;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS stewardship.remediation_log (
            log_id VARCHAR,
            exception_id VARCHAR,
            action VARCHAR,             -- ai_suggested | steward_resolved | steward_rejected
            suggested_fields VARCHAR,   -- JSON string
            rationale VARCHAR,
            suggestion_source VARCHAR,  -- ai | heuristic_fallback
            applied_fields VARCHAR,     -- JSON string, null until resolved
            steward_note VARCHAR,
            created_ts TIMESTAMP DEFAULT current_timestamp
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS stewardship.exception_status_overrides (
            exception_id VARCHAR PRIMARY KEY,
            remediation_status VARCHAR,  -- open | in_review (submitted, awaiting maker-checker approval) | resolved | rejected
            updated_ts TIMESTAMP DEFAULT current_timestamp
        );
    """)
    # Records that a steward has corrected and approved land here, then flow
    # back into silver_customers on the next pipeline run (re-validated).
    con.execute("""
        CREATE TABLE IF NOT EXISTS stewardship.remediated_records (
            exception_id VARCHAR PRIMARY KEY,
            source_system VARCHAR,
            source_record_id VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            address_line1 VARCHAR,
            address_line2 VARCHAR,
            city VARCHAR,
            state_code VARCHAR,
            postal_code VARCHAR,
            country_code VARCHAR,
            resolved_ts TIMESTAMP DEFAULT current_timestamp
        );
    """)


def _ensure_match_review_tables(con):
    """Live status overlay for gold_match_review_queue (the dbt-built static
    snapshot of borderline fuzzy-match pairs) -- same pattern as
    exception_status_overrides for exceptions_queue. Also created by
    scripts/generate_matches.py; whichever of the two runs first wins, both
    definitions must stay identical.

    'cancelled' (new): a system-written terminal status, distinct from a
    steward's own 'rejected' decision. Written when an updated version of one
    of the pair's two source records (same source_system+source_record_id)
    lands in silver while the pair's review is still open ('pending',
    'in_review', or 'confirmed' but not yet actually merged) -- the review was
    judging data that no longer exists, so it's cancelled with an explanatory
    note (steward_note) rather than left to silently judge stale data. See
    api/reprocessing.py's _cancel_stale_match_reviews / scripts/
    cancel_stale_match_reviews.py (the real-time and batch trigger points) and
    scripts/generate_matches.py's on-hold exclusion (which is what a 'pending'/
    'in_review'/unmerged-'confirmed' status actually enforces -- both source
    records sit out of matching entirely until their pair resolves one way or
    the other)."""
    con.execute("CREATE SCHEMA IF NOT EXISTS stewardship;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS stewardship.match_review_overrides (
            pair_id VARCHAR PRIMARY KEY,
            status VARCHAR,             -- 'pending' | 'in_review' (submitted, awaiting maker-checker approval) | 'confirmed' | 'rejected' | 'cancelled' (system, see docstring)
            steward_note VARCHAR,
            updated_ts TIMESTAMP DEFAULT current_timestamp
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS stewardship.match_review_log (
            log_id VARCHAR,
            pair_id VARCHAR,
            action VARCHAR,             -- 'confirmed' | 'rejected' | 'cancelled' (system, see docstring)
            steward_note VARCHAR,
            created_ts TIMESTAMP DEFAULT current_timestamp
        );
    """)


def _ensure_silver_snapshot_table(con):
    """Point-in-time snapshot of main_silver.silver_customers' identity +
    source_modified_date, kept specifically so scripts/cancel_stale_match_reviews.py
    can detect "an updated version of this source+SourceID landed in silver"
    across a batch `dbt run` -- silver has no other diff mechanism (dbt
    CREATE OR REPLACEs the whole table every run, same reason
    audit.gold_customers_snapshot exists for the gold layer / audit_pipeline_diff.py).
    Lives in the stewardship schema (not audit) because it's read/written only
    by the match-review-hold feature, not part of the gold-layer audit trail."""
    con.execute("CREATE SCHEMA IF NOT EXISTS stewardship;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS stewardship.silver_customers_snapshot (
            source_system VARCHAR,
            source_record_id VARCHAR,
            source_modified_date TIMESTAMP,
            PRIMARY KEY (source_system, source_record_id)
        );
    """)


def _ensure_audit_tables(con):
    """Append-only audit trail for the gold layer (creation, edits -- manual or
    systemic, and logical deletes). No function anywhere in this codebase issues
    an UPDATE or DELETE against audit.audit_trail, and there is no API endpoint
    that could -- that omission is the enforcement mechanism (DuckDB has no
    per-table grants to lean on here, so "not editable by any role" is an
    application-level guarantee, consistent with how gold_access read/read_write
    is already enforced in api/auth.py rather than via DB permissions).

    gold_customers_snapshot lives in this same schema specifically so it survives
    every `dbt run`'s CREATE OR REPLACE of main_gold.gold_customers -- it's how
    scripts/audit_pipeline_diff.py detects what changed across a batch rebuild."""
    con.execute("CREATE SCHEMA IF NOT EXISTS audit;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS audit.audit_trail (
            audit_id VARCHAR PRIMARY KEY,
            golden_id VARCHAR,
            change_batch_id VARCHAR,     -- groups the field-level rows from one logical operation
            event_ts TIMESTAMP,
            event_type VARCHAR,          -- 'created' | 'updated' | 'logically_deleted'
            event_source VARCHAR,        -- 'pipeline_batch' | 'portal_manual_edit' | 'steward_reprocessing'
            changed_by VARCHAR,          -- user_id, or 'system:dbt_pipeline'
            changed_by_label VARCHAR,    -- display name shown in the UI
            field_name VARCHAR,          -- null for record-level events (logical delete)
            old_value VARCHAR,
            new_value VARCHAR,
            change_reason VARCHAR,
            related_exception_id VARCHAR
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS audit.gold_customers_snapshot (
            golden_id VARCHAR PRIMARY KEY,
            first_name VARCHAR, last_name VARCHAR, email VARCHAR, phone VARCHAR,
            address_line1 VARCHAR, address_line2 VARCHAR, city VARCHAR, state_code VARCHAR,
            postal_code VARCHAR, country_code VARCHAR, source_system_count INTEGER,
            survivor_source_system VARCHAR, survivor_source_record_id VARCHAR
        );
    """)


def _ensure_governance_tables(con):
    """Generic maker-checker workflow engine tables (api/workflow_engine.py).

    workflow_definitions is deliberately a plain DB table, not a dbt seed CSV
    like column_rules.csv/matching_rules.csv -- those are batch-pipeline
    metadata refreshed by `dbt seed`, but workflow definitions need to be
    editable at runtime by governance users (the planned Rules Configuration
    screen under Data Governance), so they're seeded here once and then left
    alone on every subsequent app start -- only the first boot (empty table)
    populates DEFAULT_WORKFLOW_DEFINITIONS below.

    A workflow_type is an ordered list of steps (step_order). Each step names
    the role allowed to decide at that step and how many distinct approvers
    of that role are required before the step is satisfied (approvals_required
    > 1 means a same-level quorum, e.g. 2 different Data Owners, rather than
    sequential levels). A maker can never decide on their own submission, and
    the same approver can never cast two decisions on one instance (no
    double-counting toward a quorum) -- both enforced in workflow_engine.py,
    not here."""
    con.execute("CREATE SCHEMA IF NOT EXISTS governance;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS governance.workflow_definitions (
            workflow_type VARCHAR,
            step_order INTEGER,
            step_name VARCHAR,
            required_role VARCHAR,       -- 'admin' | 'dataSteward' | 'dataOwner' | 'businessUser'
            approvals_required INTEGER,  -- distinct approvers of required_role needed to clear this step
            is_active BOOLEAN DEFAULT true,
            PRIMARY KEY (workflow_type, step_order)
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS governance.workflow_instances (
            instance_id VARCHAR PRIMARY KEY,
            workflow_type VARCHAR,
            entity_type VARCHAR,     -- 'exception' | 'gold_customer' | 'match_pair' | 'user'
            entity_id VARCHAR,
            action_type VARCHAR,     -- 'resolve' | 'reject' | 'update' | 'confirm' | 'create' ...
            payload VARCHAR,         -- JSON: whatever the executor needs to apply the change on approval
            maker_user_id VARCHAR,
            maker_label VARCHAR,
            status VARCHAR,          -- 'pending' | 'approved' | 'rejected'
            current_step INTEGER,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            completed_ts TIMESTAMP,
            result VARCHAR           -- JSON: outcome of the executor once the workflow completes (or an error note)
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS governance.workflow_decisions (
            decision_id VARCHAR PRIMARY KEY,
            instance_id VARCHAR,
            step_order INTEGER,
            actor_user_id VARCHAR,
            actor_label VARCHAR,
            decision VARCHAR,        -- 'approved' | 'rejected'
            comment VARCHAR,
            decided_ts TIMESTAMP DEFAULT current_timestamp
        );
    """)

    # Per-workflow_type existence check (not a whole-table-empty check) so that
    # adding a new workflow_type to DEFAULT_WORKFLOW_DEFINITIONS later -- as
    # happened when reference_data_change/rules_config_change were added --
    # gets backfilled into an already-running DB on next app start, without
    # re-inserting or duplicating rows for workflow_types seeded earlier.
    existing_types = set(
        con.execute("SELECT DISTINCT workflow_type FROM governance.workflow_definitions").fetchdf()["workflow_type"]
    )
    for row in DEFAULT_WORKFLOW_DEFINITIONS:
        if row[0] in existing_types:
            continue
        con.execute("""
            INSERT INTO governance.workflow_definitions
                (workflow_type, step_order, step_name, required_role, approvals_required, is_active)
            VALUES (?, ?, ?, ?, ?, true)
        """, row)


# Seeded once per workflow_type, the first time that workflow_type is seen with
# an empty set of definitions. See _ensure_governance_tables' docstring for why
# this isn't a dbt seed CSV.
DEFAULT_WORKFLOW_DEFINITIONS = [
    # Data Stewardship: exception-queue resolve/reject. 1 level, 1 Data Owner.
    ("stewardship_remediation", 1, "Data Owner review", "dataOwner", 1),

    # Customer Portal: inline gold-record edit. 1 level, quorum of 2 Data Owners
    # (two different Data Owners must both sign off; not sequential levels).
    ("gold_record_edit", 1, "Data Owner quorum (2 of 2)", "dataOwner", 2),

    # Data Stewardship: Match Review confirm/reject. 2 sequential levels.
    ("match_review_confirmation", 1, "Data Owner review", "dataOwner", 1),
    ("match_review_confirmation", 2, "Admin sign-off", "admin", 1),

    # User Administration: create user, or update role/gold_access/is_active.
    # 1 level, 1 Admin (must be a different admin than whoever made the request).
    ("user_admin_change", 1, "Admin sign-off", "admin", 1),

    # Data Governance > Reference Data Maintenance: create/update/deactivate a
    # country or state code row. 1 level, 1 Data Owner. (Submission itself is
    # gated to dataSteward/dataOwner via auth.require_steward_or_owner; this
    # defines who approves it, which is dataOwner-only, same convention as
    # stewardship_remediation.)
    ("reference_data_change", 1, "Data Owner review", "dataOwner", 1),

    # Data Governance > Rules Configuration: create/update/deactivate a column
    # rule, matching rule, matching threshold/tier, or survivorship rule row.
    # 1 level, quorum of 2 Data Owners (higher-risk than reference data since
    # it can change what the batch pipeline rejects/matches/survives).
    ("rules_config_change", 1, "Data Owner quorum (2 of 2)", "dataOwner", 2),
]


def _ensure_reference_tables(con):
    """Reference Data Maintenance (Data Governance nav): country codes and
    state codes, DB-native and editable at runtime via the maker-checker
    'reference_data_change' workflow, instead of the old
    dbt_project/seeds/ref_country_codes.csv / ref_state_codes.csv. dbt models
    (stg_crm_customers.sql, stg_erp_customers.sql) now read these two tables
    as a source (schema 'ref') rather than via {{ ref(...) }} against a seed --
    same "Python/API writes it, dbt reads it as a source" pattern already used
    for gold_prep and governance.

    Both tables carry a human-readable *_name label (added per governance
    request) alongside the code, and an is_active flag for the "deactivate,
    never hard-delete" convention used everywhere else in this app (auth.users,
    column_rules, matching_rules). Only active rows should be treated as valid
    codes by validation.py and the dbt staging models."""
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


# Migrated from dbt_project/seeds/ref_country_codes.csv, with country_name
# added per the Reference Data Maintenance requirements.
DEFAULT_COUNTRY_CODES = [
    ("US", "United States"), ("CA", "Canada"), ("MX", "Mexico"), ("GB", "United Kingdom"),
    ("DE", "Germany"), ("FR", "France"), ("IN", "India"), ("CN", "China"),
    ("JP", "Japan"), ("AU", "Australia"),
]

# Migrated from dbt_project/seeds/ref_state_codes.csv (50 states + DC), with
# state_name added per the Reference Data Maintenance requirements.
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


def _ensure_bus_rules_tables(con):
    """Rules Configuration (Data Governance nav): column rules, matching
    rules, matching thresholds/tiers, and survivorship rules -- all DB-native
    and editable at runtime via the maker-checker 'rules_config_change'
    workflow, instead of the old dbt_project/seeds/column_rules.csv,
    matching_rules.csv, matching_thresholds.csv. dbt (gold_crosswalk.sql) and
    the batch/real-time matching code (scripts/generate_matches.py,
    api/reprocessing.py) now read matching_thresholds/matching_rules as a
    source (schema 'bus_rules') instead of a seed. column_rules currently has
    no live SQL reader anywhere (see api/validation.py, which hardcodes the
    same rule logic in Python) -- it's still moved here so it can be
    maintained through the same CRUD/approval screen as the others.

    survivorship_rules is new: exactly one active rule per gold_customers
    target_column, choosing how that column's value is picked across
    contributing source records. rule_type is one of 'most_common' (the value
    that appears in the most contributing records), 'most_complete' (prefers
    non-null/non-blank over blank), 'oldest' / 'newest' (by that source
    record's source_modified_date), or 'pattern_match' (prefers a value
    matching rule_param, a regex). Ties within a rule_type's own logic always
    fall back to 'newest' (by source_modified_date) as the universal
    tie-breaker -- that fallback is hardcoded in the evaluation engine
    (dbt_project/models/gold/gold_customers.sql), not itself a configurable
    rule, per the governance decision that there is exactly one primary rule
    per column plus one fixed tie-break, not an arbitrary stack."""
    con.execute("CREATE SCHEMA IF NOT EXISTS bus_rules;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS bus_rules.column_rules (
            rule_id VARCHAR PRIMARY KEY,
            source_system VARCHAR,
            source_column VARCHAR,
            rule_type VARCHAR,        -- not_null | regex | reference_state | reference_country | trim_case_proper | standardize_phone_us | lowercase
            rule_param VARCHAR,
            severity VARCHAR,         -- reject | correct
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
            tier_id VARCHAR,
            rule_role VARCHAR,        -- exact_match_field | similarity_text_field | blocking_key
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
            target_column VARCHAR UNIQUE,  -- gold_customers column this rule governs
            rule_type VARCHAR,             -- most_common | most_complete | oldest | newest | pattern_match
            rule_param VARCHAR,            -- pattern_match only: a regex to prefer
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


# Migrated verbatim from dbt_project/seeds/column_rules.csv.
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

# Migrated verbatim from dbt_project/seeds/matching_rules.csv.
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

# Migrated verbatim from dbt_project/seeds/matching_thresholds.csv.
DEFAULT_MATCHING_THRESHOLDS = [
    ("MT000", 0, "No-Match Baseline", "no_match_baseline", False, 0.50, None,
     "Not a real matching tier -- confidence assigned to a single-source golden record with no corroborating match from any tier (\"provisional\")"),
    ("MT001", 1, "Tier 1: Exact Match", "exact", True, 1.00, None,
     "Two records are the same customer if they share an exact match on any active exact_match_field rule (see matching_rules) after that field's transform_function is applied. Binary -- no review band; confidence is always this tier's auto_merge_threshold."),
    ("MT002", 2, "Tier 2: Fuzzy Similarity Match", "fuzzy_tfidf_cosine", True, 0.80, 0.35,
     "Candidate pairs within the same blocking_key value (see matching_rules rule_role=blocking_key) are scored by TF-IDF character n-gram cosine similarity over the concatenated similarity_text_field columns. score >= auto_merge_threshold auto-merges; review_lower_threshold <= score < auto_merge_threshold is queued to the Match Review queue for steward confirm/reject; score < review_lower_threshold is not a candidate at all."),
]

# New: one starter rule per gold_customers editable column. Defaulting every
# column to 'newest' reproduces today's existing record-level "most recently
# modified source wins" behavior exactly, so turning on attribute-level
# survivorship is a no-op for pipeline output until a governance user tunes
# individual columns to a different rule_type.
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
# Phase 1 (flexibility): governance.entity_type_registry -- single
# metadata-driven catalog for every Reference Data Maintenance / Rules
# Configuration entity type, served by GET /api/v1/governed-entities and
# consumed by both frontends to render tabs/columns/forms, instead of the
# REFERENCE_ENTITY_CONFIG / RULES_ENTITY_CONFIG dicts in api/main.py and the
# REF_DATA_TABS / RULES_TABS / REFDATA_FIELD_LABELS / RULES_FIELD_LABELS JS
# objects previously duplicated in portal_app and stewardship_app. Same
# "whichever of this file or scripts/ensure_governed_schemas.py runs first
# creates it, both stay consistent" duplication convention as
# _ensure_reference_tables/_ensure_bus_rules_tables above -- see
# scripts/ensure_governed_schemas.py's DEFAULT_ENTITY_TYPE_REGISTRY docstring
# for the full field_schema JSON shape and rationale. If you change one,
# change the other to match.
# ---------------------------------------------------------------------------

DEFAULT_ENTITY_TYPE_REGISTRY = [
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
    (
        "column_rule", "rules_config", "Column Rules", "Column Rule",
        "bus_rules.column_rules", "rule_id", "rule_id", "CR", "rules_config_change",
        None, 1,
        json.dumps([
            {"key": "source_system", "label": "Source System", "type": "select", "options": ["CRM", "ERP"]},
            {"key": "source_column", "label": "Source Column", "type": "text"},
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
            {"key": "tier_id", "label": "Tier ID", "type": "text"},
            {"key": "rule_role", "label": "Rule Role", "type": "select",
             "options": ["exact_match_field", "similarity_text_field", "blocking_key"]},
            {"key": "rule_order", "label": "Rule Order", "type": "number"},
            {"key": "source_column", "label": "Source Column", "type": "text"},
            {"key": "transform_function", "label": "Transform Function", "type": "select",
             "options": ["none", "normalize_email", "normalize_phone"]},
            {"key": "description", "label": "Description", "type": "text"},
            {"key": "is_active", "label": "Status", "type": "boolean"},
        ]),
    ),
    (
        "matching_threshold", "rules_config", "Matching Tiers", "Matching Tier",
        "bus_rules.matching_thresholds", "tier_id", "tier_order", "MT", "rules_config_change",
        None, 3,
        json.dumps([
            {"key": "tier_order", "label": "Tier Order", "type": "number"},
            {"key": "tier_name", "label": "Tier Name", "type": "text"},
            {"key": "match_method", "label": "Match Method", "type": "select",
             "options": ["exact", "fuzzy_tfidf_cosine"]},
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
            {"key": "target_column", "label": "Target Column", "type": "select", "options": [
                "first_name", "last_name", "email", "phone", "address_line1",
                "address_line2", "city", "state_code", "postal_code", "country_code",
            ]},
            {"key": "rule_type", "label": "Rule Type", "type": "select",
             "options": ["most_common", "most_complete", "oldest", "newest", "pattern_match", "shortest_value"]},
            {"key": "rule_param", "label": "Rule Param (pattern_match only, a regex)", "type": "text"},
            {"key": "description", "label": "Description", "type": "text"},
            {"key": "is_active", "label": "Status", "type": "boolean"},
        ]),
    ),
]


def _ensure_entity_type_registry(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS governance;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS governance.entity_type_registry (
            entity_type VARCHAR PRIMARY KEY,
            category VARCHAR,
            display_label VARCHAR,
            display_label_singular VARCHAR,
            table_name VARCHAR,
            pk_column VARCHAR,
            order_column VARCHAR,
            id_prefix VARCHAR,
            workflow_type VARCHAR,
            name_field VARCHAR,
            sort_order INTEGER,
            field_schema VARCHAR,
            active BOOLEAN DEFAULT true,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp
        );
    """)
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


# ---------------------------------------------------------------------------
# Domain-agnostic architecture (V4): domain_registry/domain_attributes/
# domain_source_systems/domain_relationships/user_domain_access. Same DDL
# and seed data as scripts/ensure_domain_metadata.py -- duplicated
# deliberately, same "whichever of this script or the API runs first
# creates them, both stay consistent" convention as
# _ensure_reference_tables/_ensure_bus_rules_tables above. See that
# script's module docstring for the full rationale and for why
# domain_registry seeds 'customer' (pipeline_status='live') and 'product'
# (pipeline_status='planned', a metadata-only placeholder) and why
# user_domain_access is backfilled from auth.users.gold_access without
# removing that column.
# ---------------------------------------------------------------------------

DEFAULT_DOMAINS = [
    ("customer", "Customer", "Customer", "Customers",
     "Customer master data, matched/merged from CRM and ERP source systems.",
     "live", True, 1),
    ("product", "Product", "Product", "Products",
     "Product master data (placeholder domain -- metadata only, no pipeline "
     "yet; demonstrates that the platform is not hardcoded to a single "
     "domain).",
     "planned", True, 2),
]

DEFAULT_CUSTOMER_ATTRIBUTES = [
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
    ("customer", "CRM", "CRM", 1),
    ("customer", "ERP", "ERP", 2),
]

DEFAULT_DOMAIN_RELATIONSHIPS = [
    ("REL001", "customer", "product", "Customer-Product link",
     "Illustrative link between the Customer and Product domains (e.g. "
     "purchase history) -- Product has no pipeline yet, so this "
     "relationship is metadata only for now."),
]


def _ensure_domain_tables(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS governance;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS governance.domain_registry (
            domain_key VARCHAR PRIMARY KEY,
            display_name VARCHAR,
            entity_label_singular VARCHAR,
            entity_label_plural VARCHAR,
            description VARCHAR,
            pipeline_status VARCHAR DEFAULT 'planned',
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
            data_type VARCHAR,
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
            gold_access VARCHAR DEFAULT 'none',
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR,
            PRIMARY KEY (user_id, domain_key)
        );
    """)

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

    _migrate_user_domain_access(con)


def _migrate_user_domain_access(con):
    """Backfill: copy each auth.users.gold_access value into a
    governance.user_domain_access row scoped to domain_key='customer', once
    per user. See scripts/ensure_domain_metadata.py's
    migrate_user_domain_access docstring -- same logic, kept in sync."""
    users = con.execute("SELECT user_id, gold_access FROM auth.users").fetchall()
    existing = set(
        (r[0], r[1]) for r in con.execute(
            "SELECT user_id, domain_key FROM governance.user_domain_access"
        ).fetchall()
    )
    for user_id, gold_access in users:
        if (user_id, "customer") in existing:
            continue
        con.execute("""
            INSERT INTO governance.user_domain_access (user_id, domain_key, gold_access)
            VALUES (?, 'customer', ?)
        """, (user_id, gold_access or "none"))


def run_query(sql, params=None):
    with _lock:
        con = get_con()
        if params:
            return con.execute(sql, params).fetchdf()
        return con.execute(sql).fetchdf()


def to_records(df):
    """Converts a DataFrame to a list of plain-Python dicts, safely handling
    numpy arrays (from DuckDB LIST columns) and NaN/NaT values, which FastAPI's
    default JSON encoder cannot serialize on its own."""
    import numpy as np
    import pandas as pd
    records = df.to_dict(orient="records")
    for rec in records:
        for k, v in rec.items():
            if isinstance(v, np.ndarray):
                rec[k] = v.tolist()
            elif isinstance(v, float) and np.isnan(v):
                rec[k] = None
            elif v is pd.NaT:
                rec[k] = None
    return records


def run_write(sql, params=None):
    with _lock:
        con = get_con()
        if params:
            con.execute(sql, params)
        else:
            con.execute(sql)
