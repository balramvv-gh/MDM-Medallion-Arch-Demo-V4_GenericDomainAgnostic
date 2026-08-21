"""
Real-time reprocessing path for records corrected by a data steward.

This module has two entry points, both invoked by api/main.py once a
maker-checker workflow has fully cleared, never directly off a raw API
request:
  - `reprocess_corrected_record` -- called from `_execute_stewardship_resolve`
    after a steward's correction to an exception-queue record has passed
    re-validation (see api/validation.py) and been approved.
  - `reprocess_confirmed_match_review` -- called from `_execute_match_review`
    once a steward's "confirm" decision on a Match Review pair (a suspect
    match the fuzzy-matching tier flagged as borderline) has been approved.
    Realizes the merge immediately instead of waiting for the next full
    `generate_matches.py && dbt run --select gold.*` -- see that function's
    docstring for the merge algorithm.

This module is only ever invoked by api/main.py's `resolve()` endpoint AFTER
the corrected record has already passed re-validation (see api/validation.py)
against the same reject-severity rules that routed it to the exception queue
in the first place. By the time `reprocess_corrected_record` runs, the record
is confirmed valid -- it is not this module's job to re-check that.

Phase 4 (domain-agnostic): every function here now takes a `domain_key` and
operates on the attribute-bag schema (main_silver.records / main_gold.
golden_records / gold_crosswalk's `attributes` JSON) instead of the old fixed
10-column Customer shape. `record` dicts throughout are keyed by
attribute_key (e.g. {"first_name": ..., "email": ...}), plus
source_system/source_record_id -- exactly the shape you get by unpacking a
golden_records/exceptions_queue row's `attributes` JSON column and adding
those two identity fields back in.

When a steward's correction passes re-validation, this module:
  1. Upserts the corrected record into the silver layer (main_silver.records) --
     it is now treated as canonical, on the strength of having passed re-validation.
  2. Matches the corrected record against the CURRENT set of golden records in
     this domain, using the tier-1 (exact_match_field) rules from
     bus_rules.matching_rules -- the same metadata the batch dbt
     gold_match_candidates model's tier 1 reads (today, for the customer
     domain: normalized email OR normalized phone, see bus_rules.matching_rules,
     DB-native and maintained via the Rules Configuration screen's
     maker-checker workflow). Confidence for a tier-1 match is that tier's
     auto_merge_threshold, read from bus_rules.matching_thresholds -- not a
     hardcoded literal.
  3. Runs survivorship:
       - If matched: recomputes the survivor across the whole contributing group
         (most-recently-modified source wins, tied-broken by
         governance.domain_source_systems.sort_order -- see
         _source_priority()) and updates the existing golden record + crosswalk
         in place.
       - If not matched: creates a brand-new golden record with this record as its
         sole (and therefore survivor) source.

Design notes / known simplifications (consistent with the rest of this demo):
  - Re-validation (api/validation.py) only re-checks the reject-severity rules
    from bus_rules.column_rules (the ones that determine silver eligibility),
    not the correct-severity standardization rules (proper-casing, phone
    formatting) -- those are cosmetic and don't gate silver eligibility in
    the batch pipeline either. A steward's sign-off on a now-valid record is
    still what makes the correction authoritative; the record just has to
    actually be valid first.
  - Matching is metadata-driven: the exact_match_field rows for the 'exact'
    tier in bus_rules.matching_rules determine which attributes are compared
    (and how each is normalized, via that rule's transform_function) -- see
    _load_tier1_matching_metadata() below. This module only ever evaluates
    the 'exact' tier; a 'fuzzy_tfidf_cosine' tier (if any) is intentionally
    skipped here -- fitting a TF-IDF vectorizer per API request would be a
    real latency/complexity cost for a demo-scoped real-time path.
  - The corrected record's "modified" timestamp is stamped as the moment of
    correction. Combined with the recency-wins survivorship rule, this means a fresh
    steward correction will typically become the new survivor -- which is usually the
    desired behavior (a just-verified value should be trusted as current).
  - Matching compares the corrected record's tier-1 attribute values against
    each existing golden record's own `attributes` JSON, not against every
    historical non-survivor member of that group. This mirrors the batch
    gold layer's record-level (not attribute-level) survivorship design; a
    follow-on iteration could match against every underlying source row for
    full precision.
  - New golden IDs are assigned by incrementing the current max FOR THIS
    DOMAIN ('GOLD-{DOMAIN}-00001', scoped per domain since match_group_id
    numbering from a full `dbt run` is only unique within a domain, see
    dbt_project/models/gold/golden_records.sql) -- this can diverge from the
    numbering a full `dbt run` would produce from scratch (dbt's dense_rank
    is order-dependent on the complete silver set). Both paths remain
    internally consistent; the numbers themselves just aren't guaranteed
    portable between them.
"""
import json
import re
import uuid
from datetime import datetime

import pandas as pd

from db import run_query, run_write
import audit as audit_mod

# Same transform_function registry as scripts/generate_matches.py (Python-side
# values). SQL_TRANSFORMS below is the equivalent for building the WHERE
# clause against DuckDB directly -- kept alongside deliberately so both stay
# in lockstep as new transform_function values are added to matching_rules.
PY_TRANSFORMS = {
    "none": lambda v: v,
    "normalize_email": lambda v: (v or "").strip().lower(),
    "normalize_phone": lambda v: re.sub(r"[^0-9]", "", v or ""),
}

# Each lambda takes a SQL EXPRESSION (not necessarily a bare column name --
# here it's always `attributes->>'<attribute_key>'`) and wraps it.
SQL_TRANSFORMS = {
    "none": lambda expr: expr,
    "normalize_email": lambda expr: f"lower(trim({expr}))",
    "normalize_phone": lambda expr: f"regexp_replace(coalesce({expr},''), '[^0-9]', '', 'g')",
}


def _attr_json_field(golden_records_alias_col, attribute_key):
    """SQL expression extracting one attribute from a golden_records.attributes
    JSON column reference (e.g. 'attributes') for a given attribute_key."""
    return f"{golden_records_alias_col}->>'{attribute_key}'"


def _source_priority(domain_key):
    """Reads governance.domain_source_systems.sort_order for this domain --
    the tie-break preference order for 'which source wins when candidates are
    otherwise equally ranked', replacing the old hardcoded 'CRM preferred'.
    Lower sort_order wins ties. A source_system with no row (shouldn't
    normally happen) sorts last."""
    df = run_query(
        "SELECT source_system_key, sort_order FROM governance.domain_source_systems "
        "WHERE domain_key = ? AND active",
        [domain_key],
    )
    return {row["source_system_key"]: row["sort_order"] for _, row in df.iterrows()}


def _load_survivorship_rules(domain_key):
    """Reads bus_rules.survivorship_rules (at most one active rule per
    (domain_key, attribute_key)), mirroring
    dbt_project/models/gold/gold_survivorship_winners.sql. An attribute with
    no active rule row defaults to 'newest', same as that dbt model's
    coalesce(rule_type, 'newest')."""
    df = run_query(
        "SELECT attribute_key, rule_type, rule_param FROM bus_rules.survivorship_rules "
        "WHERE active AND domain_key = ?",
        [domain_key],
    )
    return {row["attribute_key"]: {"rule_type": row["rule_type"], "rule_param": row["rule_param"]}
            for _, row in df.iterrows()}


# Handler registry for survivorship rule_type, mirroring RULE_TYPE_HANDLERS in
# api/validation.py and dbt's macros/rule_type_expr.sql (used by
# gold_survivorship_winners.sql's CASE dispatch). Each handler takes
# (raw_value, clean_value, modified_date, rule_param, freq) and returns a
# numeric score -- higher wins, per-candidate, for one attribute_key. This is
# the real-time reprocessing path's equivalent of that dbt model's `when`
# branches, and the two MUST stay in lockstep (same rule_type -> same
# ranking): adding a new rule_type here without also adding it there (or vice
# versa) would silently make the two paths disagree on who wins an attribute.
SURVIVORSHIP_RANK_HANDLERS = {
    "newest": lambda v, clean_v, modified_date, rule_param, freq: _epoch(modified_date),
    "oldest": lambda v, clean_v, modified_date, rule_param, freq: -_epoch(modified_date),
    "most_complete": lambda v, clean_v, modified_date, rule_param, freq: (
        1 if clean_v is not None else 0
    ),
    "pattern_match": lambda v, clean_v, modified_date, rule_param, freq: (
        1 if clean_v is not None and rule_param and re.search(rule_param, str(v)) else 0
    ),
    "most_common": lambda v, clean_v, modified_date, rule_param, freq: (
        freq.get(clean_v, 0) if clean_v is not None else 0
    ),
    "shortest_value": lambda v, clean_v, modified_date, rule_param, freq: (
        -1 * len(clean_v) if clean_v is not None else -999999
    ),
}


def _pick_attribute_winners(candidates, rules, source_priority):
    """candidates: list of {"source_system", "source_record_id", "modified_date", "data"}
    where "data" is that candidate's attribute dict (attribute_key -> value).

    Returns (pivoted, winning_attributes_by_source):
      pivoted: dict of attribute_key -> winning value across all candidates.
      winning_attributes_by_source: dict of (source_system, source_record_id) ->
        set of attribute_key names that source won.

    The attribute_key set evaluated is the UNION of keys present across every
    candidate's "data" dict -- not a hardcoded field list -- so this works
    unchanged for any domain's attribute set (see dbt_project/models/gold/
    gold_survivorship_winners.sql for the equivalent batch-path design,
    which unpivots governance.domain_attributes the same way).

    Evaluates each attribute independently against its own rule_type,
    dispatched through SURVIVORSHIP_RANK_HANDLERS above, tie-broken by
    source_modified_date desc then domain_source_systems.sort_order -- this
    must stay in lockstep with dbt's gold_survivorship_winners.sql (same
    rule, evaluated in Python here for the real-time reprocessing path
    instead of SQL for the batch path). An unrecognized rule_type raises
    rather than silently scoring 0 for every candidate (which would make it
    look configured while doing nothing) -- same fail-loud principle as the
    dbt side."""
    attribute_keys = sorted(set().union(*(c["data"].keys() for c in candidates))) if candidates else []
    pivoted = {}
    winning_attributes_by_source = {(c["source_system"], c["source_record_id"]): set() for c in candidates}

    for attr in attribute_keys:
        rule = rules.get(attr) or {"rule_type": "newest", "rule_param": None}
        rule_type = rule["rule_type"] or "newest"
        rule_param = rule["rule_param"]

        handler = SURVIVORSHIP_RANK_HANDLERS.get(rule_type)
        if handler is None:
            raise ValueError(
                f"Unrecognized survivorship rule_type '{rule_type}' for attribute_key '{attr}' -- "
                f"no handler registered in reprocessing.SURVIVORSHIP_RANK_HANDLERS. "
                f"Known rule_types: {', '.join(sorted(SURVIVORSHIP_RANK_HANDLERS))}."
            )

        freq = {}
        if rule_type == "most_common":
            for c in candidates:
                v = (str(c["data"].get(attr)).strip() if c["data"].get(attr) is not None else "") or None
                if v is not None:
                    freq[v] = freq.get(v, 0) + 1

        def rank_key(c, handler=handler, rule_param=rule_param, freq=freq, attr=attr):
            v = c["data"].get(attr)
            clean_v = (str(v).strip() if v is not None else "") or None
            return handler(v, clean_v, c["modified_date"], rule_param, freq)

        def sort_key(c, attr=attr):
            source_pref = source_priority.get(c["source_system"], 999999)
            return (-rank_key(c), -_epoch(c["modified_date"]), source_pref)

        winner = min(candidates, key=sort_key)
        pivoted[attr] = winner["data"].get(attr)
        winning_attributes_by_source[(winner["source_system"], winner["source_record_id"])].add(attr)

    return pivoted, winning_attributes_by_source


def _load_tier1_matching_metadata(domain_key):
    """Reads the active 'exact' tier from bus_rules.matching_thresholds and its
    exact_match_field rules from bus_rules.matching_rules, both scoped to this
    domain. Raises if no active exact tier is configured for the domain --
    this module has nothing meaningful to do without one."""
    tiers = run_query("""
        SELECT * FROM bus_rules.matching_thresholds
        WHERE active AND is_match_tier AND match_method = 'exact' AND domain_key = ?
        ORDER BY tier_order
    """, [domain_key])
    if tiers.empty:
        raise RuntimeError(
            f"No active tier with match_method='exact' found in bus_rules.matching_thresholds "
            f"for domain '{domain_key}' -- real-time reprocessing has no matching rule to apply."
        )
    tier1 = tiers.iloc[0]

    rules = run_query("""
        SELECT * FROM bus_rules.matching_rules
        WHERE active AND tier_id = ? AND rule_role = 'exact_match_field' AND domain_key = ?
        ORDER BY rule_order
    """, [tier1["tier_id"], domain_key])

    return float(tier1["auto_merge_threshold"]), rules.to_dict(orient="records")


def _load_baseline_confidence(domain_key):
    """Reads the non-tier 'no_match_baseline' row from bus_rules.matching_thresholds
    for this domain -- the confidence assigned to a golden record with no
    corroborating match at all (single source, e.g. a brand-new record
    created by this module when no existing golden record matched). Mirrors
    the batch gold_crosswalk.sql fallback for the identical situation, so
    the two paths can't drift on this value."""
    baseline = run_query("""
        SELECT * FROM bus_rules.matching_thresholds
        WHERE active AND NOT is_match_tier AND match_method = 'no_match_baseline' AND domain_key = ?
    """, [domain_key])
    if baseline.empty:
        raise RuntimeError(
            f"No active 'no_match_baseline' row found in bus_rules.matching_thresholds for domain '{domain_key}'."
        )
    return float(baseline.iloc[0]["auto_merge_threshold"])


def _epoch(dt):
    """Comparable numeric timestamp; treats missing/unparseable dates as oldest possible."""
    if dt is None:
        return -1
    ts = pd.to_datetime(dt, errors="coerce")
    if pd.isna(ts):
        return -1
    return ts.timestamp()


def _cancel_stale_match_reviews(domain_key, source_system, source_record_id, now) -> list:
    """If the just-upserted (domain_key, source_system, source_record_id) is
    one of the two sides of a Match Review pair whose review is still open --
    'pending', 'in_review', or 'confirmed' but not yet actually merged into
    gold (the batch step that realizes a confirmed decision,
    scripts/generate_matches.py, hasn't run since it was confirmed) -- that
    review was judging data that no longer exists as of this correction.
    Cancel it with an explanatory note rather than let it keep judging stale
    data or silently merge on outdated values later. A 'rejected' or
    already-'cancelled' pair is left alone (already terminal); a
    'confirmed'-and-already-merged pair is also left alone (nothing left to
    cancel -- the decision already took full effect).

    Pair identity is resolved primarily from gold_prep.match_review_candidates
    (written by scripts/generate_matches.py), which doesn't depend on
    `dbt run --select gold.*` having happened since the last batch build --
    same reasoning as generate_matches.py's own on-hold-key resolution (see
    that script's docstring, "Match Review hold"). Falls back to the
    dbt-published main_gold.gold_match_review_queue for any pair_id not found
    there.

    Returns a list of {"pair_id", "note"} dicts, one per pair actually
    cancelled (normally 0 or 1)."""
    try:
        candidates = run_query("""
            SELECT pair_id, source_system_a, source_record_id_a, source_system_b, source_record_id_b
            FROM gold_prep.match_review_candidates
            WHERE domain_key = ?
              AND ((source_system_a = ? AND source_record_id_a = ?)
               OR (source_system_b = ? AND source_record_id_b = ?))
        """, [domain_key, source_system, source_record_id, source_system, source_record_id])
    except Exception:
        candidates = pd.DataFrame()

    try:
        queue_candidates = run_query("""
            SELECT pair_id, source_system_a, source_record_id_a, source_system_b, source_record_id_b
            FROM main_gold.gold_match_review_queue
            WHERE domain_key = ?
              AND ((source_system_a = ? AND source_record_id_a = ?)
               OR (source_system_b = ? AND source_record_id_b = ?))
        """, [domain_key, source_system, source_record_id, source_system, source_record_id])
    except Exception:
        queue_candidates = pd.DataFrame()

    if not queue_candidates.empty:
        known_pair_ids = set(candidates["pair_id"]) if not candidates.empty else set()
        extra = queue_candidates[~queue_candidates["pair_id"].isin(known_pair_ids)]
        candidates = pd.concat([candidates, extra], ignore_index=True) if not extra.empty else candidates

    if candidates.empty:
        return []

    overrides = run_query(
        "SELECT pair_id, status FROM stewardship.match_review_overrides WHERE pair_id IN ("
        + ",".join(["?"] * len(candidates)) + ")",
        list(candidates["pair_id"]),
    )
    status_by_pair = dict(zip(overrides["pair_id"], overrides["status"])) if not overrides.empty else {}

    note = (
        f"Updated version of this record ({source_system}-{source_record_id}) "
        f"received on {now.strftime('%m/%d/%Y')}"
    )
    cancelled = []
    for _, row in candidates.iterrows():
        pair_id = row["pair_id"]
        status = status_by_pair.get(pair_id, "pending")  # no override row yet == still pending

        if status in ("rejected", "cancelled"):
            continue

        if status == "confirmed":
            merged = run_query("""
                SELECT 1
                FROM main_gold.gold_crosswalk a
                JOIN main_gold.gold_crosswalk b ON a.golden_id = b.golden_id
                WHERE a.domain_key = ? AND a.source_system = ? AND a.source_record_id = ?
                  AND b.domain_key = ? AND b.source_system = ? AND b.source_record_id = ?
            """, [domain_key, row["source_system_a"], row["source_record_id_a"],
                  domain_key, row["source_system_b"], row["source_record_id_b"]])
            if not merged.empty:
                continue  # already realized -- nothing to cancel

        run_write("""
            INSERT OR REPLACE INTO stewardship.match_review_overrides (pair_id, status, steward_note)
            VALUES (?, 'cancelled', ?)
        """, [pair_id, note])
        run_write("""
            INSERT INTO stewardship.match_review_log (log_id, pair_id, action, steward_note)
            VALUES (?, ?, 'cancelled', ?)
        """, [str(uuid.uuid4()), pair_id, note])
        cancelled.append({"pair_id": pair_id, "note": note})

    return cancelled


def _record_attributes(record: dict) -> dict:
    """Strips the identity/meta keys off `record`, leaving just the
    attribute_key -> value pairs that belong in `attributes` JSON."""
    return {k: v for k, v in record.items() if k not in ("source_system", "source_record_id", "exception_id", "domain_key")}


def reprocess_corrected_record(domain_key: str, record: dict, actor: dict = None) -> dict:
    """Entry point called after a steward resolves an exception. `record` must
    contain source_system, source_record_id, and the (possibly corrected)
    attribute_key-keyed fields for this domain. `actor` is the authenticated
    steward/owner (api/auth.py's _public_user shape) -- used to attribute the
    resulting audit trail entry; falls back to a generic 'system' attribution
    if not supplied."""
    source_system = record["source_system"]
    source_record_id = record["source_record_id"]
    now = datetime.utcnow()
    attributes = _record_attributes(record)

    # --- (a) Upsert into the silver layer -----------------------------------
    run_write(
        "DELETE FROM main_silver.records WHERE domain_key = ? AND source_system = ? AND source_record_id = ?",
        [domain_key, source_system, source_record_id],
    )
    run_write("""
        INSERT INTO main_silver.records
            (domain_key, source_system, source_record_id, attributes, silver_load_ts)
        VALUES (?, ?, ?, ?, ?)
    """, [domain_key, source_system, source_record_id, json.dumps(attributes), now])

    # --- (a.1) Cancel any Match Review still open against the PREVIOUS version
    # of this source record -- it was judging data that no longer exists. ---
    cancelled_match_reviews = _cancel_stale_match_reviews(domain_key, source_system, source_record_id, now)

    # --- (b) Match against the current gold set in this domain, using tier-1
    # exact_match_field rules from bus_rules.matching_rules (see
    # _load_tier1_matching_metadata) --
    tier1_confidence, exact_rules = _load_tier1_matching_metadata(domain_key)

    conditions, params = [], []
    for rule in exact_rules:
        attr = rule["attribute_key"]
        transform = rule["transform_function"] or "none"
        sql_expr = SQL_TRANSFORMS.get(transform, SQL_TRANSFORMS["none"])(_attr_json_field("attributes", attr))
        val = PY_TRANSFORMS.get(transform, PY_TRANSFORMS["none"])(attributes.get(attr))
        conditions.append(f"(? != '' AND {sql_expr} = ?)")
        params += [val, val]

    matched = pd.DataFrame()
    if conditions:
        where_clause = " OR ".join(conditions)
        matched = run_query(
            f"SELECT DISTINCT golden_id FROM main_gold.golden_records WHERE domain_key = ? AND ({where_clause})",
            [domain_key] + params,
        )

    # --- (c) Survivorship: update existing golden record, or create a new one --
    if not matched.empty:
        golden_id = matched.iloc[0]["golden_id"]
        result = _update_existing_golden_record(domain_key, golden_id, source_system, source_record_id, record, now, tier1_confidence, actor)
    else:
        # No existing golden record matched -- this source becomes its own new
        # golden record with no corroboration, so it gets the no-match baseline
        # confidence (bus_rules.matching_thresholds, match_method='no_match_baseline'),
        # not the tier-1 confidence -- consistent with the batch gold_crosswalk.sql
        # fallback for the identical single-source situation.
        baseline_confidence = _load_baseline_confidence(domain_key)
        result = _create_new_golden_record(domain_key, source_system, source_record_id, record, now, baseline_confidence, actor)

    result["cancelled_match_reviews"] = cancelled_match_reviews
    return result


def _flatten_golden_row(row: dict) -> dict:
    """golden_records rows come back with an `attributes` JSON string column;
    audit.py's log_creation/log_update want a flat dict (attribute_key ->
    value, plus the fixed meta fields) to diff, same shape
    scripts/audit_pipeline_diff.py builds for the batch pipeline's own audit
    writes."""
    attrs = json.loads(row["attributes"]) if row.get("attributes") else {}
    flat = dict(attrs)
    flat["source_system_count"] = row.get("source_system_count")
    flat["survivor_source_system"] = row.get("survivor_source_system")
    flat["survivor_source_record_id"] = row.get("survivor_source_record_id")
    return flat


def _create_new_golden_record(domain_key, source_system, source_record_id, record, now, new_record_confidence, actor=None) -> dict:
    golden_id = _next_golden_id(domain_key)
    attributes = _record_attributes(record)

    run_write("""
        INSERT INTO main_gold.golden_records
            (golden_id, domain_key, match_group_id, attributes, source_system_count,
             survivor_source_system, survivor_source_record_id, gold_curated_ts)
        VALUES (?, ?, NULL, ?, 1, ?, ?, ?)
    """, [golden_id, domain_key, json.dumps(attributes), source_system, source_record_id, now])

    # Sole contributing source: wins every attribute outright.
    run_write("""
        INSERT INTO main_gold.gold_crosswalk
            (golden_id, domain_key, source_system, source_record_id, match_confidence_score,
             winning_attributes, is_survivor_record, crosswalk_created_ts)
        VALUES (?, ?, ?, ?, ?, ?, true, ?)
    """, [golden_id, domain_key, source_system, source_record_id, new_record_confidence,
          sorted(attributes.keys()), now])

    new_row = run_query("SELECT * FROM main_gold.golden_records WHERE golden_id = ?", [golden_id]).iloc[0].to_dict()
    audit_mod.log_creation(
        golden_id, _flatten_golden_row(new_row), event_source="steward_reprocessing",
        changed_by=(actor or {}).get("user_id", "system:steward_reprocessing"),
        changed_by_label=(actor or {}).get("full_name") or (actor or {}).get("username", "steward"),
        change_reason=f"Created via real-time reprocessing of exception {record.get('exception_id', 'unknown')}",
        related_exception_id=record.get("exception_id"), event_ts=now,
    )

    return {
        "action": "created_new_golden_record",
        "golden_id": golden_id,
        "is_survivor": True,
        "matched_existing_sources": 0,
    }


def _update_existing_golden_record(domain_key, golden_id, source_system, source_record_id, record, now, tier1_confidence, actor=None) -> dict:
    old_row_df = run_query("SELECT * FROM main_gold.golden_records WHERE golden_id = ?", [golden_id])
    old_record = _flatten_golden_row(old_row_df.iloc[0].to_dict()) if not old_row_df.empty else {}

    # Idempotency: drop any stale crosswalk row for this exact source before recomputing.
    run_write(
        "DELETE FROM main_gold.gold_crosswalk WHERE golden_id = ? AND source_system = ? AND source_record_id = ?",
        [golden_id, source_system, source_record_id],
    )

    existing_crosswalk = run_query(
        "SELECT * FROM main_gold.gold_crosswalk WHERE golden_id = ?", [golden_id]
    )

    candidates = []
    for _, row in existing_crosswalk.iterrows():
        s = run_query(
            "SELECT * FROM main_silver.records WHERE domain_key = ? AND source_system = ? AND source_record_id = ?",
            [domain_key, row["source_system"], row["source_record_id"]],
        )
        if not s.empty:
            srow = s.iloc[0]
            attrs = json.loads(srow["attributes"]) if srow["attributes"] else {}
            candidates.append({
                "source_system": row["source_system"],
                "source_record_id": row["source_record_id"],
                "modified_date": attrs.get("source_modified_date"),
                "data": attrs,
            })

    candidates.append({
        "source_system": source_system,
        "source_record_id": source_record_id,
        "modified_date": now,
        "data": _record_attributes(record),
    })

    distinct_sources = {c["source_system"] for c in candidates}

    # Attribute-level survivorship: each attribute is picked independently
    # per its own bus_rules.survivorship_rules rule_type, tie-broken by
    # source_modified_date desc then domain_source_systems.sort_order. Must
    # stay in lockstep with dbt's gold_survivorship_winners.sql (same rule,
    # evaluated here in Python for this real-time path).
    rules = _load_survivorship_rules(domain_key)
    source_priority = _source_priority(domain_key)
    pivoted, winning_attributes_by_source = _pick_attribute_winners(candidates, rules, source_priority)

    # "Primary" survivor_source_system/survivor_source_record_id kept for
    # backward compatibility (existing API/UI fields expect a single survivor
    # per golden record) -- whichever source won the most attributes,
    # tie-broken the same way (most recent modified_date, then domain_source_systems.sort_order).
    def primary_sort_key(c):
        key = (c["source_system"], c["source_record_id"])
        attrs_won = len(winning_attributes_by_source[key])
        source_pref = source_priority.get(c["source_system"], 999999)
        return (-attrs_won, -_epoch(c["modified_date"]), source_pref)

    survivor = min(candidates, key=primary_sort_key)

    run_write("""
        UPDATE main_gold.golden_records SET
            attributes = ?, source_system_count = ?, survivor_source_system = ?,
            survivor_source_record_id = ?, gold_curated_ts = ?
        WHERE golden_id = ?
    """, [json.dumps(pivoted), len(distinct_sources),
          survivor["source_system"], survivor["source_record_id"], now, golden_id])

    # Recompute winning_attributes/is_survivor_record for every contributing
    # source (which attributes each one wins may have shifted with this
    # correction).
    for c in candidates:
        key = (c["source_system"], c["source_record_id"])
        won_attrs = sorted(winning_attributes_by_source[key])
        is_surv = len(won_attrs) > 0
        run_write(
            "DELETE FROM main_gold.gold_crosswalk WHERE golden_id = ? AND source_system = ? AND source_record_id = ?",
            [golden_id, c["source_system"], c["source_record_id"]],
        )
        run_write("""
            INSERT INTO main_gold.gold_crosswalk
                (golden_id, domain_key, source_system, source_record_id, match_confidence_score,
                 winning_attributes, is_survivor_record, crosswalk_created_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [golden_id, domain_key, c["source_system"], c["source_record_id"], tier1_confidence, won_attrs, is_surv, now])

    new_source_is_survivor = len(winning_attributes_by_source[(source_system, source_record_id)]) > 0

    new_row = run_query("SELECT * FROM main_gold.golden_records WHERE golden_id = ?", [golden_id]).iloc[0].to_dict()
    audit_mod.log_update(
        golden_id, old_record=old_record, new_record=_flatten_golden_row(new_row),
        event_source="steward_reprocessing",
        changed_by=(actor or {}).get("user_id", "system:steward_reprocessing"),
        changed_by_label=(actor or {}).get("full_name") or (actor or {}).get("username", "steward"),
        change_reason=(
            f"Attribute-level survivorship recomputed via real-time reprocessing of "
            f"exception {record.get('exception_id', 'unknown')} (primary survivor: "
            f"{survivor['source_system']}:{survivor['source_record_id']})"
        ),
        related_exception_id=record.get("exception_id"), event_ts=now,
    )

    return {
        "action": "updated_existing_golden_record",
        "golden_id": golden_id,
        "is_survivor": new_source_is_survivor,
        "matched_existing_sources": len(candidates) - 1,
    }


def _next_golden_id(domain_key: str) -> str:
    """'GOLD-{DOMAIN}-00001', scoped to this domain -- see module docstring
    on why golden IDs are domain-qualified since Phase 3."""
    df = run_query("SELECT golden_id FROM main_gold.golden_records WHERE domain_key = ?", [domain_key])
    prefix = f"GOLD-{domain_key.upper()}-"
    max_n = 0
    for gid in df["golden_id"]:
        s = str(gid)
        if not s.startswith(prefix):
            continue
        try:
            n = int(s[len(prefix):])
            max_n = max(max_n, n)
        except ValueError:
            continue
    return f"{prefix}{max_n + 1:05d}"


def _golden_id_for_source(domain_key, source_system, source_record_id):
    """Looks up the current golden_id a (domain_key, source_system,
    source_record_id) contributes to via main_gold.gold_crosswalk. Returns
    None if not found -- shouldn't normally happen (every silver record is
    assigned a golden_id, whether a singleton or a multi-source merge, by
    either the batch pipeline or a prior real-time reprocessing action), but
    callers treat a miss as a configuration problem to raise on, not a
    silent no-op."""
    df = run_query(
        "SELECT golden_id FROM main_gold.gold_crosswalk WHERE domain_key = ? AND source_system = ? AND source_record_id = ?",
        [domain_key, source_system, source_record_id],
    )
    return df.iloc[0]["golden_id"] if not df.empty else None


def _golden_id_sort_key(golden_id):
    """Numeric sort key for a 'GOLD-CUSTOMER-00007'-style id -- 'smallest
    golden ID' means numerically smallest within its domain segment, not
    merely lexicographically smallest. Parses the LAST '-'-separated segment
    (the zero-padded number), which is robust to the domain segment's own
    length varying between domains -- unlike indexing a fixed split()
    position, which pre-Phase-3's 'GOLD-00007' (no domain segment) format
    could get away with but this one can't."""
    try:
        return int(str(golden_id).rsplit("-", 1)[-1])
    except (IndexError, ValueError):
        return float("inf")


def reprocess_confirmed_match_review(pair_id: str, actor: dict = None) -> dict:
    """Entry point called by api/main.py's `_execute_match_review` once a
    Match Review pair's "confirm" decision has cleared its full
    maker-checker chain (Data Owner, then Admin). Replaces the previous
    behavior of only writing the decision to
    stewardship.match_review_overrides and waiting for the next full
    `generate_matches.py && dbt run --select gold.*` to actually realize the
    merge -- this mirrors, for a confirmed suspect match, what
    `reprocess_corrected_record` already does for a corrected exception.

    domain_key is read off the Match Review pair itself
    (main_gold.gold_match_review_queue.domain_key) -- callers don't need to
    already know it.

    Both sides of a confirmed pair already belong to some golden record --
    every silver record is assigned one, whether a brand-new singleton or an
    existing multi-source merge. The auto-processing steps:
      1. Runs attribute-level survivorship (the same
         bus_rules.survivorship_rules-driven, per-attribute logic
         `_update_existing_golden_record` uses) across the UNION of every
         source record already contributing to EITHER side's golden
         record -- not just the two paired records themselves -- producing
         the merged group's surviving value for every gold attribute except
         golden_id itself.
      2. The surviving golden_id is the smaller (by numeric suffix, within
         the shared domain segment -- both sides of a pair are always the
         same domain) of the two sides' current golden records.
      3. Since a Match Review pair only ever forms between two records that
         were NOT already in the same match group, this is normally two
         distinct golden records: the losing one is retired -- its
         main_gold.golden_records row is deleted and logged to the audit
         trail as 'logically_deleted' (the same convention
         scripts/audit_pipeline_diff.py uses when a batch rebuild drops a
         golden_id that no longer exists) -- and every crosswalk row that
         pointed at either golden record is rewritten to point at the
         surviving one only.
      4. Returns a result dict describing the merge; the caller is what
         actually marks the stewardship record complete (writing
         'confirmed' to stewardship.match_review_overrides, which it does
         BEFORE calling this function -- see _execute_match_review -- so
         the human decision stands and is recorded even if this mechanical
         merge step itself fails; a failure surfaces in the workflow
         instance's `result.executor_error` rather than being silently lost,
         same posture as every other on_approved callback in this app).

    If the two sides somehow already resolved to the SAME golden record
    (e.g. an earlier reprocessing action already merged them via a
    different path), there is nothing to merge -- returns immediately with
    action='already_merged' instead of re-running survivorship for no
    reason. Raises RuntimeError (fail loud, not a silent no-op) if the pair
    itself can't be found, or if a side has no golden_id at all.

    Known simplification, consistent with the rest of this module: if a
    member of the losing golden record's group is itself the subject of a
    DIFFERENT still-open Match Review pair, that other pair stays open under
    its new (surviving) golden_id -- this function only resolves the pair it
    was asked to realize, mirroring `_cancel_stale_match_reviews`' own scope
    (only the just-changed record's own pairs, not every downstream
    consequence of a merge)."""
    now = datetime.utcnow()

    pair_df = run_query(
        "SELECT * FROM main_gold.gold_match_review_queue WHERE pair_id = ?", [pair_id]
    )
    if pair_df.empty:
        raise RuntimeError(
            f"No Match Review candidate found in main_gold.gold_match_review_queue for "
            f"pair_id={pair_id} -- cannot realize this confirmation."
        )
    pair = pair_df.iloc[0]
    domain_key = pair["domain_key"]
    side_a = (pair["source_system_a"], pair["source_record_id_a"])
    side_b = (pair["source_system_b"], pair["source_record_id_b"])
    similarity_score = float(pair["similarity_score"])

    golden_id_a = _golden_id_for_source(domain_key, *side_a)
    golden_id_b = _golden_id_for_source(domain_key, *side_b)
    if golden_id_a is None or golden_id_b is None:
        raise RuntimeError(
            f"Match Review pair {pair_id} has a side with no current golden_id in "
            f"main_gold.gold_crosswalk ({side_a}: {golden_id_a}, {side_b}: {golden_id_b}) -- "
            f"cannot realize this confirmation until both sides are represented in gold."
        )

    if golden_id_a == golden_id_b:
        return {"action": "already_merged", "golden_id": golden_id_a, "pair_id": pair_id}

    surviving_golden_id, retiring_golden_id = sorted([golden_id_a, golden_id_b], key=_golden_id_sort_key)

    # --- (1) Gather every source record currently contributing to EITHER
    # side's golden record -- the full "approved suspect match group", not
    # just the two paired records -- and run attribute-level survivorship
    # across all of them. ---
    crosswalk = run_query(
        "SELECT * FROM main_gold.gold_crosswalk WHERE golden_id IN (?, ?)",
        [surviving_golden_id, retiring_golden_id],
    )

    candidates = []
    prior_confidence = {}
    for _, row in crosswalk.iterrows():
        key = (row["source_system"], row["source_record_id"])
        prior_confidence[key] = row["match_confidence_score"]
        s = run_query(
            "SELECT * FROM main_silver.records WHERE domain_key = ? AND source_system = ? AND source_record_id = ?",
            [domain_key, row["source_system"], row["source_record_id"]],
        )
        if not s.empty:
            srow = s.iloc[0]
            attrs = json.loads(srow["attributes"]) if srow["attributes"] else {}
            candidates.append({
                "source_system": row["source_system"],
                "source_record_id": row["source_record_id"],
                "modified_date": attrs.get("source_modified_date"),
                "data": attrs,
            })

    if not candidates:
        raise RuntimeError(
            f"Neither golden record in Match Review pair {pair_id} ({surviving_golden_id}, "
            f"{retiring_golden_id}) has any contributing source record still present in "
            f"main_silver.records -- nothing to survive."
        )

    old_survivor_row = run_query(
        "SELECT * FROM main_gold.golden_records WHERE golden_id = ?", [surviving_golden_id]
    )
    old_record = _flatten_golden_row(old_survivor_row.iloc[0].to_dict()) if not old_survivor_row.empty else {}

    rules = _load_survivorship_rules(domain_key)
    source_priority = _source_priority(domain_key)
    pivoted, winning_attributes_by_source = _pick_attribute_winners(candidates, rules, source_priority)
    distinct_sources = {c["source_system"] for c in candidates}

    def primary_sort_key(c):
        key = (c["source_system"], c["source_record_id"])
        attrs_won = len(winning_attributes_by_source[key])
        source_pref = source_priority.get(c["source_system"], 999999)
        return (-attrs_won, -_epoch(c["modified_date"]), source_pref)

    survivor = min(candidates, key=primary_sort_key)

    # --- (2)/(3) Write the merged record: update the surviving golden_id in
    # place with the freshly-survived values, retire the losing golden_id,
    # and rewrite the crosswalk so every contributing record now points at
    # the surviving golden_id only. ---
    run_write("""
        UPDATE main_gold.golden_records SET
            attributes = ?, source_system_count = ?, survivor_source_system = ?,
            survivor_source_record_id = ?, gold_curated_ts = ?
        WHERE golden_id = ?
    """, [json.dumps(pivoted), len(distinct_sources),
          survivor["source_system"], survivor["source_record_id"], now, surviving_golden_id])

    run_write(
        "DELETE FROM main_gold.gold_crosswalk WHERE golden_id IN (?, ?)",
        [surviving_golden_id, retiring_golden_id],
    )

    pair_keys = {side_a, side_b}
    for c in candidates:
        key = (c["source_system"], c["source_record_id"])
        won_attrs = sorted(winning_attributes_by_source[key])
        is_surv = len(won_attrs) > 0
        # The two records this pair directly connects get the pair's own
        # similarity score as their confidence (that's the edge that
        # actually justifies their membership); every other pre-existing
        # member keeps whatever confidence it already had -- this merge
        # doesn't re-derive confidence for connections it didn't establish.
        confidence = similarity_score if key in pair_keys else prior_confidence.get(key, similarity_score)
        run_write("""
            INSERT INTO main_gold.gold_crosswalk
                (golden_id, domain_key, source_system, source_record_id, match_confidence_score,
                 winning_attributes, is_survivor_record, crosswalk_created_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [surviving_golden_id, domain_key, c["source_system"], c["source_record_id"], confidence, won_attrs, is_surv, now])

    run_write("DELETE FROM main_gold.golden_records WHERE golden_id = ?", [retiring_golden_id])

    new_row = run_query(
        "SELECT * FROM main_gold.golden_records WHERE golden_id = ?", [surviving_golden_id]
    ).iloc[0].to_dict()
    audit_mod.log_update(
        surviving_golden_id, old_record=old_record, new_record=_flatten_golden_row(new_row),
        event_source="steward_reprocessing",
        changed_by=(actor or {}).get("user_id", "system:steward_reprocessing"),
        changed_by_label=(actor or {}).get("full_name") or (actor or {}).get("username", "steward"),
        change_reason=(
            f"Attribute-level survivorship recomputed via real-time reprocessing of confirmed "
            f"Match Review pair {pair_id} (merged {retiring_golden_id} into this record; "
            f"primary survivor: {survivor['source_system']}:{survivor['source_record_id']})"
        ),
        event_ts=now,
    )
    audit_mod.log_logical_delete(
        retiring_golden_id,
        event_source="steward_reprocessing",
        changed_by=(actor or {}).get("user_id", "system:steward_reprocessing"),
        changed_by_label=(actor or {}).get("full_name") or (actor or {}).get("username", "steward"),
        change_reason=(
            f"Retired via real-time reprocessing of confirmed Match Review pair {pair_id} -- "
            f"merged into {surviving_golden_id} (smaller golden_id survives)."
        ),
        event_ts=now,
    )

    return {
        "action": "merged",
        "golden_id": surviving_golden_id,
        "retired_golden_id": retiring_golden_id,
        "contributing_sources": len(candidates),
    }
