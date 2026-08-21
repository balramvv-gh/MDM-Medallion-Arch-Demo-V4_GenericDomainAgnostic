"""
Re-validation for steward-corrected records, run at "Approve & Resolve" time.

A record only reaches the exception queue because it failed one or more of the
reject-severity rules in bus_rules.column_rules (see models/silver/
stg_crm_customers.sql, stg_erp_customers.sql, and silver_all_staged.sql for
the batch-pipeline implementation of these same checks). This module re-runs
that same reject-severity subset against a steward's corrected fields before
the record is allowed to flow into reprocessing (silver upsert + match/merge).

Phase 2 (flexibility): this module now reads bus_rules.column_rules directly
instead of hardcoding a fixed check per field (the old _EMAIL_RE constant,
fixed not-blank checks, etc.). Each active, reject-severity rule row is
dispatched through RULE_TYPE_HANDLERS, keyed by rule_type -- the same
handler-registry shape as TRANSFORM_FUNCTIONS in scripts/generate_matches.py.
Editing a rule's rule_param via Rules Configuration (e.g. loosening/
tightening R003's email regex) takes effect here immediately, with no code
change, exactly as it already did in the batch dbt path once Phase 2's
dbt macros (dbt_project/macros/rule_type_expr.sql) landed.

Design notes:
  - Only reject-severity checks are re-verified here (not the correct-severity
    standardization rules like proper-casing or phone formatting) -- those are
    cosmetic and don't gate silver eligibility in the batch pipeline either.
  - By the time a record reaches the exception queue, CRM and ERP records have
    already been staged into the same canonical field names (first_name,
    last_name, email, phone, state_code, country_code), so a single set of
    checks covers both source systems. bus_rules.column_rules rows are keyed
    by (source_system, source_column) though -- CANONICAL_FIELD_MAP below
    translates each source-specific column name to the shared canonical field
    a corrected record's dict keys use.
  - State/country reference lists are read from the same DB-native reference
    tables the batch pipeline validates against (ref.ref_state_codes /
    ref.ref_country_codes, maintained via the Reference Data Maintenance
    screen's maker-checker workflow), not hardcoded here, so the two paths
    can't drift. Queried fresh on every call (no caching) since these tables
    can change at runtime, unlike the old static seed CSVs.
  - Scope boundary, deliberately: a rule row whose source_column has no entry
    in CANONICAL_FIELD_MAP is skipped here (there's no corresponding key in a
    corrected record's dict to check it against) rather than raised as an
    error -- unlike the dbt-side macros, which raise a hard compiler error for
    exactly this situation. That asymmetry is intentional: the dbt models
    build the canonical fields FROM the raw source columns, so an unmapped
    reject rule there is a real gap in the pipeline; here, a corrected
    record has already been staged into canonical fields, so the only rule
    rows that could ever matter are the ones that map onto those fields --
    an ERP-only rule like R009 (full_name, not a canonical field) simply
    isn't relevant to re-validating an already-staged record, the same way
    it doesn't produce a real 'full_name' key in `record`. See ERP's R009
    special case in stg_erp_customers.sql for where that rule *does* apply.
  - Unrecognized rule_type still fails loudly: RULE_TYPE_HANDLERS raises
    ValueError for a rule_type it doesn't know how to check, consistent with
    Phase 0's "fail loudly on unrecognized rule_type" principle -- a stewards
    screen. It will never let a record silently skip a check whose logic
    nobody actually implemented.
  - The reject reason strings intentionally match the wording already shown in
    the stewardship UI's "FAILED VALIDATION" box (silver_all_staged.sql), so a
    "still failing" alert reads consistently with what the steward saw when
    the record first landed in the queue.
"""
import re

from db import run_query

# bus_rules.column_rules.source_column (per source_system) -> the canonical
# field name a corrected record's dict uses. Only source_columns that map
# 1:1 onto an already-staged canonical field belong here -- see the
# "Scope boundary" design note above for why ERP's full_name (R009) is
# deliberately absent.
CANONICAL_FIELD_MAP = {
    ("CRM", "first_name"): "first_name",
    ("CRM", "last_name"): "last_name",
    ("CRM", "email"): "email",
    ("CRM", "state"): "state_code",
    ("CRM", "country"): "country_code",
    ("ERP", "email_addr"): "email",
    ("ERP", "contact_phone"): "phone",
    ("ERP", "state_code"): "state_code",
    ("ERP", "country_code"): "country_code",
}

# Reason strings kept identical to the pre-Phase-2 hardcoded messages so the
# stewardship UI's "still failing" alerts don't change wording.
CANONICAL_FIELD_REASON = {
    "first_name": "R001/R009: missing first name",
    "last_name": "R002/R009: missing last name",
    "email": "R003/R010: invalid email format",
    "phone": "R011: missing phone",
    "state_code": "R004/R012: invalid state code",
    "country_code": "R005/R013: invalid country code",
}


def _valid_state_codes():
    df = run_query("SELECT state_code FROM ref.ref_state_codes WHERE is_active")
    return set(df["state_code"].tolist())


def _valid_country_codes():
    df = run_query("SELECT country_code FROM ref.ref_country_codes WHERE is_active")
    return set(df["country_code"].tolist())


def _check_not_null(value, rule_param):
    return not value


def _check_regex(value, rule_param):
    if not value:
        return True
    if not rule_param:
        raise ValueError("rule_type 'regex' requires a rule_param (the pattern) to be set")
    return re.match(rule_param, value) is None


def _check_reference_state(value, rule_param):
    return not value or value not in _valid_state_codes()


def _check_reference_country(value, rule_param):
    return not value or value not in _valid_country_codes()


# Handler registry: rule_type -> callable(value, rule_param) -> bool (True
# means the rule is VIOLATED). Mirrors TRANSFORM_FUNCTIONS in
# scripts/generate_matches.py -- adding a genuinely new reject rule_type
# means registering one function here, not editing an if/elif chain.
RULE_TYPE_HANDLERS = {
    "not_null": _check_not_null,
    "regex": _check_regex,
    "reference_state": _check_reference_state,
    "reference_country": _check_reference_country,
}


def _active_reject_rules():
    """Every active, reject-severity bus_rules.column_rules row, mapped onto
    the canonical field it re-validates (rows with no canonical mapping --
    see CANONICAL_FIELD_MAP -- are dropped; see module docstring)."""
    df = run_query(
        "SELECT source_system, source_column, rule_type, rule_param "
        "FROM bus_rules.column_rules WHERE active AND severity = 'reject'"
    )
    rules = []
    for row in df.to_dict("records"):
        canonical = CANONICAL_FIELD_MAP.get((row["source_system"], row["source_column"]))
        if canonical is None:
            continue
        rules.append((canonical, row["rule_type"], row["rule_param"]))
    return rules


def validate_record(record: dict) -> list[str]:
    """Returns the list of reject reasons still present in `record` (empty
    list means the record now passes every reject-severity rule and is safe
    to reprocess)."""
    values = {
        "first_name": (record.get("first_name") or "").strip(),
        "last_name": (record.get("last_name") or "").strip(),
        "email": (record.get("email") or "").strip(),
        "phone": (record.get("phone") or "").strip(),
        "state_code": (record.get("state_code") or "").strip().upper(),
        "country_code": (record.get("country_code") or "").strip().upper(),
    }

    violated_fields = set()
    for canonical_field, rule_type, rule_param in _active_reject_rules():
        handler = RULE_TYPE_HANDLERS.get(rule_type)
        if handler is None:
            raise ValueError(
                f"Unrecognized column_rules.rule_type '{rule_type}' for canonical field "
                f"'{canonical_field}' -- no handler registered in validation.RULE_TYPE_HANDLERS. "
                f"Known rule_types: {', '.join(sorted(RULE_TYPE_HANDLERS))}."
            )
        if handler(values[canonical_field], rule_param):
            violated_fields.add(canonical_field)

    # Stable, human-readable order regardless of the rules' DB row order.
    return [
        CANONICAL_FIELD_REASON[f]
        for f in ("first_name", "last_name", "email", "phone", "state_code", "country_code")
        if f in violated_fields
    ]
