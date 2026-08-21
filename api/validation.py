"""
Re-validation for steward-corrected records, run at "Approve & Resolve" time.

A record only reaches the exception queue because it failed one or more of the
reject-severity rules in bus_rules.column_rules (see dbt_project/macros/
validation_block.sql and models/silver/stg_source_records.sql for the
batch-pipeline implementation of these same checks). This module re-runs that
same reject-severity subset against a steward's corrected fields before the
record is allowed to flow into reprocessing (silver upsert + match/merge).

Phase 4 (domain-agnostic): this module now takes a `domain_key` and validates
against that domain's `attribute_key`-keyed bus_rules.column_rules rows
(scripts/migrate_column_rules_to_domain.py), instead of a hardcoded 6-field
Customer dict. Each active, reject-severity rule row is dispatched through
RULE_TYPE_HANDLERS, keyed by rule_type -- the same handler-registry shape as
TRANSFORM_FUNCTIONS in scripts/generate_matches.py and rule_type_expr.sql's
dbt macro. Editing a rule's rule_param via Rules Configuration takes effect
here immediately, with no code change.

Design notes:
  - Only reject-severity checks are re-verified here (not the correct-severity
    standardization rules like proper-casing or phone formatting) -- those are
    cosmetic and don't gate silver eligibility in the batch pipeline either.
  - `record` is a plain dict keyed by attribute_key (e.g. {"first_name": ...,
    "email": ...}) -- the caller (api/main.py) is responsible for unpacking a
    golden_records/exceptions_queue row's `attributes` JSON column into this
    shape before calling validate_record(). A rule row with attribute_key NULL
    (e.g. R009, ERP's composite full_name check -- see
    scripts/migrate_column_rules_to_domain.py's docstring) is skipped here,
    the same "not a plain attribute-level check" scope boundary
    dbt_project/macros/validation_block.sql documents for its own composite
    handling; this module doesn't re-implement that composite-split case
    (a corrected record is already staged into canonical attributes, so
    there's no raw "full_name" key left to re-check).
  - State/country reference lists are read from the same DB-native reference
    tables the batch pipeline validates against (ref.ref_state_codes /
    ref.ref_country_codes, maintained via the Reference Data Maintenance
    screen's maker-checker workflow), not hardcoded here, so the two paths
    can't drift. Queried fresh on every call (no caching) since these tables
    can change at runtime. Comparison is case-insensitive (value uppercased
    before the membership check) so a steward's freshly-typed correction
    doesn't have to already be in canonical case.
  - Unrecognized rule_type still fails loudly: RULE_TYPE_HANDLERS raises
    ValueError for a rule_type it doesn't know how to check, consistent with
    this project's "fail loudly on unrecognized rule_type" principle -- it
    will never let a record silently skip a check whose logic nobody
    actually implemented.
  - Reason strings are "<rule_id>: <description>" straight from the governed
    rule row, the same format dbt_project/macros/validation_block.sql now
    produces for the batch pipeline -- so a steward's "still failing" alert
    reads consistently with what they saw when the record first landed in
    the queue, regardless of which path (batch or steward-corrected)
    generated it.
"""
from db import run_query

# Handler registry: rule_type -> callable(value, rule_param) -> bool (True
# means the rule is VIOLATED). Mirrors TRANSFORM_FUNCTIONS in
# scripts/generate_matches.py and dbt_project/macros/rule_type_expr.sql --
# adding a genuinely new reject rule_type means registering one function
# here (and a matching branch in rule_type_expr.sql), not editing an
# if/elif chain.
import re


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
    return not value or value.upper() not in _valid_state_codes()


def _check_reference_country(value, rule_param):
    return not value or value.upper() not in _valid_country_codes()


RULE_TYPE_HANDLERS = {
    "not_null": _check_not_null,
    "regex": _check_regex,
    "reference_state": _check_reference_state,
    "reference_country": _check_reference_country,
}


def _active_reject_rules(domain_key: str):
    """Every active, reject-severity bus_rules.column_rules row for this
    domain that maps onto a real attribute_key (a row with attribute_key
    NULL -- a composite/hand-written check, see module docstring -- is
    dropped). Multiple rows on the same attribute_key are all returned; the
    caller ORs their violations together, same as
    dbt_project/macros/validation_block.sql."""
    df = run_query(
        "SELECT rule_id, attribute_key, rule_type, rule_param, description "
        "FROM bus_rules.column_rules WHERE active AND severity = 'reject' "
        "AND domain_key = ? AND attribute_key IS NOT NULL",
        [domain_key],
    )
    return df.to_dict("records")


def validate_record(domain_key: str, record: dict) -> list[str]:
    """Returns the list of reject reasons still present in `record` (a dict
    keyed by attribute_key) for this domain -- empty list means the record
    now passes every reject-severity rule and is safe to reprocess."""
    rules = _active_reject_rules(domain_key)

    # attribute_key -> "<rule_id>: <description>" of the first violated rule
    # on that attribute (stable order: DB row order via rule_id, same as
    # every other rule-ordering convention in this project).
    reasons_by_attribute = {}
    for rule in rules:
        attribute_key = rule["attribute_key"]
        if attribute_key in reasons_by_attribute:
            continue  # already violated via an earlier rule on this same attribute
        handler = RULE_TYPE_HANDLERS.get(rule["rule_type"])
        if handler is None:
            raise ValueError(
                f"Unrecognized column_rules.rule_type '{rule['rule_type']}' for attribute "
                f"'{attribute_key}' (domain '{domain_key}') -- no handler registered in "
                f"validation.RULE_TYPE_HANDLERS. Known rule_types: {', '.join(sorted(RULE_TYPE_HANDLERS))}."
            )
        value = (record.get(attribute_key) or "").strip() if isinstance(record.get(attribute_key), str) else record.get(attribute_key)
        if handler(value, rule["rule_param"]):
            reasons_by_attribute[attribute_key] = f"{rule['rule_id']}: {rule['description']}"

    # Stable, human-readable order: by attribute_key, matching the order
    # governance.domain_attributes would list them in for any domain.
    return [reasons_by_attribute[k] for k in sorted(reasons_by_attribute)]
