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
    con.execute("""
        CREATE TABLE IF NOT EXISTS bus_rules.column_rules (
            rule_id VARCHAR PRIMARY KEY,
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
            target_column VARCHAR UNIQUE,
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
            # Phase 0 upgraded match_method from unconstrained free text to a
            # select constrained to methods scripts/generate_matches.py
            # actually implements -- see RULES_FIELD_OPTIONS in api/main.py.
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


def main():
    con = duckdb.connect(DB_PATH)
    ensure_reference_tables(con)
    ensure_bus_rules_tables(con)
    ensure_orchestration_tables(con)
    ensure_monitoring_tables(con)
    ensure_entity_type_registry(con)
    print("ref.*, bus_rules.*, and governance.orchestration_*/monitoring_* schemas/tables ready.")


if __name__ == "__main__":
    main()
