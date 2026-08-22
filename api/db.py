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
        _ensure_auth_tables(_con)
        _ensure_audit_tables(_con)
        _ensure_governance_tables(_con)
        _ensure_reference_tables(_con)
        _ensure_bus_rules_tables(_con)
        _ensure_entity_type_registry(_con)
        _ensure_domain_tables(_con)
        # Flush the ensure/migrate DDL above out of the WAL immediately,
        # rather than leaving it to accumulate for however long this process
        # (a long-lived FastAPI server, potentially) stays up. Observed
        # empirically: this installed DuckDB build (1.5.4) can fail to
        # replay a WAL that contains an ALTER TABLE ... ADD COLUMN ... DEFAULT
        # entry (raises an internal "GetDefaultDatabase" assertion) if the
        # file is reopened by a fresh process before that WAL is ever
        # checkpointed -- e.g. the API process being stopped without an
        # explicit shutdown checkpoint (see the shutdown handler in
        # api/main.py, which covers the normal-exit path; this call is the
        # belt to that suspenders, since a crash bypasses shutdown handlers
        # entirely). A crashed/killed process can't be prevented from ever
        # happening, so minimizing the window during which a
        # not-yet-checkpointed ALTER sits in the WAL is the practical
        # mitigation available here.
        _con.execute("CHECKPOINT")
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
    # back into the real-time reprocessing path (api/reprocessing.py's
    # reprocess_corrected_record) immediately, and into main_silver.records on
    # the next full pipeline run (re-validated either way).
    #
    # Phase 4 (domain-agnostic): attribute-bag shaped like every other table
    # in this codebase since Phase 2 -- domain_key + a JSON attributes column
    # (attribute_key -> corrected value) instead of a fixed 10-column Customer
    # shape, so a steward's correction to ANY domain's record can be logged
    # here with no schema change.
    con.execute("""
        CREATE TABLE IF NOT EXISTS stewardship.remediated_records (
            exception_id VARCHAR PRIMARY KEY,
            domain_key VARCHAR,
            source_system VARCHAR,
            source_record_id VARCHAR,
            attributes VARCHAR,   -- JSON: attribute_key -> corrected value
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


# Phase 4 (domain-agnostic) note: stewardship.silver_records_snapshot (point-
# in-time identity + source_modified_date snapshot of main_silver.records,
# domain_key-scoped) and audit.golden_records_snapshot (same idea for the gold
# layer) are NOT created here. Both are owned entirely by the one standalone
# batch script that reads/writes them (scripts/cancel_stale_match_reviews.py
# and scripts/audit_pipeline_diff.py respectively), which already creates its
# own table via CREATE TABLE IF NOT EXISTS -- the same "standalone scripts
# duplicate DDL rather than importing db.py" convention documented in
# api/audit.py's module docstring. The API layer never reads either table, so
# there's no reason for get_con() to also create them.


def _ensure_audit_tables(con):
    """Append-only audit trail for the gold layer (creation, edits -- manual or
    systemic, and logical deletes). No function anywhere in this codebase issues
    an UPDATE or DELETE against audit.audit_trail, and there is no API endpoint
    that could -- that omission is the enforcement mechanism (DuckDB has no
    per-table grants to lean on here, so "not editable by any role" is an
    application-level guarantee, consistent with how gold_access read/read_write
    is already enforced in api/auth.py rather than via DB permissions).

    audit.golden_records_snapshot (domain_key-scoped, attributes JSON) is
    created and owned by scripts/audit_pipeline_diff.py, not here -- see the
    note above _ensure_stewardship_tables' silver-snapshot comment for why."""
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
    # Phase 2/3 (domain-agnostic): every bus_rules table below carries
    # domain_key (+ attribute_key/target_column, already-existing columns
    # that stay as-is) so a rule can target ANY domain's attributes, not
    # just Customer's fixed column set -- see scripts/
    # migrate_column_rules_to_domain.py and migrate_matching_rules_to_domain.py
    # for the full migration rationale. Baked directly into the CREATE TABLE
    # here (rather than only an ALTER TABLE in those standalone scripts) so a
    # brand-new database created purely by starting the API -- with neither
    # script ever run -- still gets the correct domain-scoped shape; the
    # backfill migration below (_migrate_bus_rules_to_domain) mirrors both
    # scripts' logic so it's also safe to run against a pre-Phase-2 DB opened
    # directly by the API.
    con.execute("""
        CREATE TABLE IF NOT EXISTS bus_rules.column_rules (
            rule_id VARCHAR PRIMARY KEY,
            domain_key VARCHAR,
            attribute_key VARCHAR,
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
            domain_key VARCHAR,
            attribute_key VARCHAR,
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
            target_column VARCHAR,  -- golden_records attribute this rule governs (legacy name, see attribute_key)
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

    _migrate_bus_rules_to_domain(con)
    _ensure_product_bus_rules(con)


# (source_system, source_column) -> (domain_key, attribute_key), covering
# every DEFAULT_COLUMN_RULES row (R001-R015). Mirrors
# scripts/migrate_column_rules_to_domain.py's _CANONICAL_MAP exactly -- kept
# in sync with that file.
_COLUMN_RULES_CANONICAL_MAP = {
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
    # ERP's full_name (R009) intentionally has NO entry -- see
    # scripts/migrate_column_rules_to_domain.py's docstring.
}

# R016: promotes CRM's previously-hardcoded "phone is required" check into a
# real governed rule. See scripts/migrate_column_rules_to_domain.py's
# docstring -- kept in sync with that file's _NEW_RULES.
_COLUMN_RULES_NEW_ROWS = [
    ("R016", "CRM", "phone", "not_null", None, "reject",
     "Phone is required (promoted from a pre-Phase-2 hardcoded check -- "
     "see scripts/migrate_column_rules_to_domain.py)."),
]


def _migrate_bus_rules_to_domain(con):
    """Backfills domain_key (+ attribute_key/target_column) onto every
    bus_rules.* row that doesn't have one yet -- identical logic to
    scripts/migrate_column_rules_to_domain.py and
    migrate_matching_rules_to_domain.py, duplicated here (same "API and
    standalone scripts each stay self-contained" convention used elsewhere
    in this codebase, e.g. api/reprocessing.py's PY_TRANSFORMS/SQL_TRANSFORMS)
    so that opening the duckdb file directly via the API -- without either
    migration script having been run first -- still ends up in the correct
    domain-scoped shape. Idempotent: only touches rows where domain_key IS
    NULL, so re-running (or running after the standalone scripts already
    did) is a safe no-op."""
    rows = con.execute(
        "SELECT rule_id, source_system, source_column FROM bus_rules.column_rules WHERE domain_key IS NULL"
    ).fetchall()
    for rule_id, source_system, source_column in rows:
        attribute_key = _COLUMN_RULES_CANONICAL_MAP.get((source_system, source_column))
        if attribute_key is None:
            # R009 (ERP full_name): composite check, deliberately left
            # without an attribute_key -- see _CANONICAL_MAP's comment above.
            con.execute("UPDATE bus_rules.column_rules SET domain_key = 'customer' WHERE rule_id = ?", [rule_id])
        else:
            con.execute(
                "UPDATE bus_rules.column_rules SET domain_key = 'customer', attribute_key = ? WHERE rule_id = ?",
                [attribute_key, rule_id],
            )
    existing_ids = set(r[0] for r in con.execute("SELECT rule_id FROM bus_rules.column_rules").fetchall())
    for rule_id, source_system, attribute_key, rule_type, rule_param, severity, description in _COLUMN_RULES_NEW_ROWS:
        if rule_id in existing_ids:
            continue
        con.execute("""
            INSERT INTO bus_rules.column_rules
                (rule_id, domain_key, source_system, attribute_key, source_column,
                 rule_type, rule_param, severity, description)
            VALUES (?, 'customer', ?, ?, ?, ?, ?, ?, ?)
        """, (rule_id, source_system, attribute_key, attribute_key, rule_type, rule_param, severity, description))

    con.execute("""
        UPDATE bus_rules.matching_rules SET domain_key = 'customer', attribute_key = source_column
        WHERE domain_key IS NULL
    """)
    con.execute("""
        UPDATE bus_rules.matching_thresholds SET domain_key = 'customer'
        WHERE domain_key IS NULL
    """)
    con.execute("""
        UPDATE bus_rules.survivorship_rules SET domain_key = 'customer', attribute_key = target_column
        WHERE domain_key IS NULL
    """)


# ---------------------------------------------------------------------------
# Product domain onboarding: bus_rules seed data for domain_key='product'.
# Unlike DEFAULT_COLUMN_RULES/MATCHING_RULES/MATCHING_THRESHOLDS/
# SURVIVORSHIP_RULES below (Customer's original rows, inserted without a
# domain_key -- domain_key is backfilled afterward by
# _migrate_bus_rules_to_domain above), these Product rows carry domain_key/
# attribute_key directly at insert time, since Product is onboarded after
# domain_key already exists as a first-class column -- there is no
# "pre-domain-key" era to replay for a brand-new domain. Mirrors
# scripts/ensure_governed_schemas.py's DEFAULT_PRODUCT_*/ensure_product_bus_rules
# exactly -- if you change one, change the other to match.
# ---------------------------------------------------------------------------

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

# One rule per Product attribute that participates in survivorship. Varied
# across every rule_type this project supports, including the new
# 'source_priority' ("select if source is X"). Rule-type choice per attribute
# depends on which of ERP/PLM/SUPPLIER actually map it (see
# DEFAULT_PRODUCT_SOURCE_FIELD_MAPPINGS in scripts/ensure_domain_metadata.py)
# -- see that file's DEFAULT_PRODUCT_SURVIVORSHIP_RULES comment for the full
# per-attribute rationale (why 'newest'/'oldest' are safe only for the three
# attributes every source maps, and every other attribute uses
# 'most_complete' or 'source_priority' naming its one sole mapper instead).
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


def _ensure_product_bus_rules(con):
    """Seeds Product's bus_rules rows using a PER-RULE_ID existence check --
    unlike the DEFAULT_COLUMN_RULES/etc. seed blocks above, which only ever
    fire on a whole-table-empty check. This database already has Customer's
    rows in every one of these four tables, so that whole-table-empty check
    never fires again; this function is what actually gets Product's rows
    inserted. Idempotent: only inserts rule_ids that don't already exist.
    Mirrors scripts/ensure_governed_schemas.py's ensure_product_bus_rules --
    if you change one, change the other to match."""
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
    # Phase 4 (domain-agnostic): every rules_config field_schema below adds
    # domain_key (which domain this rule governs) and attribute_key (which of
    # that domain's governance.domain_attributes rows it targets) as plain
    # text fields -- not a hardcoded 'select' of literal domain/attribute
    # names (the survivorship_rule target_column field used to list
    # Customer's exact column names here, which is exactly the kind of
    # domain-name-in-code this phase eliminates). A steward types the
    # domain_key/attribute_key value directly; GET /api/v1/domains and
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

    # field_schema can drift from DEFAULT_ENTITY_TYPE_REGISTRY as this file
    # evolves (e.g. Phase 4 added domain_key/attribute_key to the four
    # rules_config entity types) -- this table has no write endpoint anywhere
    # in the API, so re-syncing every row's field_schema to the current
    # DEFAULT_ENTITY_TYPE_REGISTRY on every app start is always safe.
    for row in DEFAULT_ENTITY_TYPE_REGISTRY:
        con.execute(
            "UPDATE governance.entity_type_registry SET field_schema = ? WHERE entity_type = ?",
            [row[-1], row[0]],
        )


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
     "Product master data, matched/merged from ERP (commercial + operational), "
     "PLM (engineering), and Supplier (the supplier's own spec sheet) source systems.",
     "live", True, 2),
]

# Existing installs may already have a 'product' row seeded back when it was
# still pipeline_status='planned' (metadata-only placeholder) -- the
# insert-if-missing loop below never touches an existing row, so onboarding
# Product's real pipeline needs an explicit, idempotent flip alongside it.
# Kept in sync with scripts/ensure_domain_metadata.py's identical constant.
_PRODUCT_PIPELINE_STATUS_UPGRADE_SQL = """
    UPDATE governance.domain_registry
    SET pipeline_status = 'live',
        description = 'Product master data, matched/merged from ERP (commercial + '
                       'operational), PLM (engineering), and Supplier (the supplier''s '
                       'own spec sheet) source systems.'
    WHERE domain_key = 'product' AND pipeline_status != 'live'
"""

# is_dashboard_breakdown: see scripts/ensure_domain_metadata.py's copy of
# this constant for the full docstring -- must stay in sync.
DEFAULT_CUSTOMER_ATTRIBUTES = [
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

# Canonical attribute set for domain_key='product' -- kept in sync with
# scripts/ensure_domain_metadata.py's identical constant (see that file for
# the full field-by-field rationale).
DEFAULT_PRODUCT_ATTRIBUTES = [
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

_DASHBOARD_BREAKDOWN_ATTRS = {
    ("customer", "state_code"), ("customer", "country_code"),
    ("product", "category"), ("product", "product_status"),
}

ALL_DOMAIN_ATTRIBUTES = (
    [("customer",) + attr for attr in DEFAULT_CUSTOMER_ATTRIBUTES]
    + [("product",) + attr for attr in DEFAULT_PRODUCT_ATTRIBUTES]
)

DEFAULT_CUSTOMER_SOURCE_SYSTEMS = [
    ("customer", "CRM", "CRM", 1),
    ("customer", "ERP", "ERP", 2),
]

DEFAULT_PRODUCT_SOURCE_SYSTEMS = [
    ("product", "ERP", "ERP", 1),
    ("product", "PLM", "PLM", 2),
    ("product", "SUPPLIER", "Supplier", 3),
]

DEFAULT_DOMAIN_RELATIONSHIPS = [
    ("REL001", "customer", "product", "Customer-Product link",
     "Illustrative link between the Customer and Product domains (e.g. "
     "purchase history) -- Product has no pipeline yet, so this "
     "relationship is metadata only for now."),
]

# Phase 2: how each domain/source system's raw field names map onto
# canonical attribute_keys, plus which standardization transform to apply.
# See scripts/ensure_domain_metadata.py's matching section for the full
# rationale -- duplicated here per this file's schema-duplication
# convention (kept in sync with that script).
DEFAULT_SOURCE_FIELD_MAPPINGS = [
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
    # source_created_date / source_modified_date: see
    # scripts/ensure_domain_metadata.py's mirror of this constant for the
    # full rationale (ERP deliberately has no source_created_date mapping).
    ("SFM020", "customer", "CRM", "source_created_date", "attributes->>'created_date'", "none", None),
    ("SFM021", "customer", "CRM", "source_modified_date", "attributes->>'modified_date'", "none", None),
    ("SFM022", "customer", "ERP", "source_modified_date", "attributes->>'last_updated'", "none", None),
]

# Product domain: kept in sync with scripts/ensure_domain_metadata.py's
# identical constant (see that file for the full field-by-field rationale --
# three sources with different raw field names, asymmetric coverage, and the
# ERP-gross_weight-vs-PLM-weight_kg deliberate type/format mismatch resolved
# via the new 'numeric_extract' transform_function).
DEFAULT_PRODUCT_SOURCE_FIELD_MAPPINGS = [
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

    # Idempotent upgrade for an existing install that already seeded 'product'
    # back when it was pipeline_status='planned'.
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

    for dk, ak in _DASHBOARD_BREAKDOWN_ATTRS:
        con.execute("""
            UPDATE governance.domain_attributes
            SET is_dashboard_breakdown = true
            WHERE domain_key = ? AND attribute_key = ? AND NOT is_dashboard_breakdown
        """, [dk, ak])

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
            source_expr VARCHAR,
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

    _migrate_user_domain_access(con)
    _migrate_product_domain_access(con)


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


def _migrate_product_domain_access(con):
    """Backfill: every user with a governance.user_domain_access row for
    'customer' gets an equivalent row for 'product' (same gold_access level),
    so Product going live doesn't silently lock everyone out of it. See
    scripts/ensure_domain_metadata.py's migrate_product_domain_access
    docstring -- same logic, kept in sync. No-ops gracefully if auth.users
    doesn't exist, though get_con() always creates it before this runs."""
    auth_users_exists = con.execute("""
        SELECT count(*) FROM information_schema.tables
        WHERE table_schema = 'auth' AND table_name = 'users'
    """).fetchone()[0]
    if not auth_users_exists:
        return
    customer_access = con.execute(
        "SELECT user_id, gold_access FROM governance.user_domain_access WHERE domain_key = 'customer'"
    ).fetchall()
    existing = set(
        r[0] for r in con.execute(
            "SELECT user_id FROM governance.user_domain_access WHERE domain_key = 'product'"
        ).fetchall()
    )
    for user_id, gold_access in customer_access:
        if user_id in existing:
            continue
        con.execute("""
            INSERT INTO governance.user_domain_access (user_id, domain_key, gold_access)
            VALUES (?, 'product', ?)
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
