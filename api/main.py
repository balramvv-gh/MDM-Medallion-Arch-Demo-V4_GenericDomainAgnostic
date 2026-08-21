import json
import uuid
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from db import run_query, run_write, to_records
from ai_remediation import suggest_remediation
import lineage as lineage_mod
import auth as auth_mod
import audit as audit_mod
import workflow_engine as wf_mod
from reprocessing import reprocess_corrected_record, reprocess_confirmed_match_review
from validation import validate_record

BASE_DIR = Path(__file__).resolve().parent.parent

app = FastAPI(
    title="MDM Demo API",
    description="Gold-layer REST API, lineage/impact analysis, and data stewardship endpoints "
                "for the MDM medallion architecture demo.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

class LoginRequest(BaseModel):
    username: str
    password: str


@app.post("/api/v1/auth/login")
def login(body: LoginRequest):
    result = auth_mod.authenticate(body.username, body.password)
    if result is None:
        raise HTTPException(401, "Invalid username or password")
    return result


@app.post("/api/v1/auth/logout")
def logout_endpoint(authorization: str = Header(None)):
    if authorization and authorization.startswith("Bearer "):
        auth_mod.logout(authorization.removeprefix("Bearer ").strip())
    return {"status": "logged_out"}


@app.get("/api/v1/auth/me")
def me(user: dict = Depends(auth_mod.get_current_user)):
    return user


# ---------------------------------------------------------------------------
# Generic maker-checker workflow endpoints (api/workflow_engine.py).
#
# Data Stewardship remediation, Match Review, Portal gold-record edits, and
# User Administration create/update all submit into this same engine rather
# than applying immediately -- see the workflow_type-specific "submit" calls
# further down and their _execute_*/_rollback_* callbacks. These three
# endpoints are the one place any authenticated user goes to see what's
# awaiting their decision, see their own past submissions, and decide.
# ---------------------------------------------------------------------------

def _wf_error(e: wf_mod.WorkflowError) -> HTTPException:
    if isinstance(e, wf_mod.WorkflowNotFoundError):
        return HTTPException(404, str(e))
    if isinstance(e, wf_mod.WorkflowPermissionError):
        return HTTPException(403, str(e))
    if isinstance(e, wf_mod.WorkflowStateError):
        return HTTPException(409, str(e))
    return HTTPException(400, str(e))


def _paginate_list(items: list, limit: int, offset: int) -> dict:
    """Shared pagination envelope for endpoints whose underlying data is
    already fetched as a plain Python list (rather than sliced in SQL) --
    total is the full unpaginated count, items is just the requested page."""
    return {"total": len(items), "limit": limit, "offset": offset, "items": items[offset:offset + limit]}


@app.get("/api/v1/workflows/pending")
def list_pending_approvals(limit: int = 10, offset: int = 0, user: dict = Depends(auth_mod.get_current_user)):
    return _paginate_list(wf_mod.list_pending_for_user(user), limit, offset)


@app.get("/api/v1/workflows/mine")
def list_my_submissions(limit: int = 10, offset: int = 0, user: dict = Depends(auth_mod.get_current_user)):
    return _paginate_list(wf_mod.list_mine(user["user_id"]), limit, offset)


@app.get("/api/v1/workflows/{instance_id}")
def get_workflow_instance(instance_id: str, user: dict = Depends(auth_mod.get_current_user)):
    try:
        return wf_mod.get_instance(instance_id)
    except wf_mod.WorkflowError as e:
        raise _wf_error(e)


class WorkflowDecisionRequest(BaseModel):
    decision: str  # 'approved' | 'rejected'
    comment: Optional[str] = None


@app.post("/api/v1/workflows/{instance_id}/decide")
def decide_workflow_instance(instance_id: str, body: WorkflowDecisionRequest,
                              user: dict = Depends(auth_mod.get_current_user)):
    if body.decision not in ("approved", "rejected"):
        raise HTTPException(400, "decision must be 'approved' or 'rejected'")
    try:
        instance = wf_mod.get_instance(instance_id)
    except wf_mod.WorkflowError as e:
        raise _wf_error(e)

    on_approved, on_rejected = _WORKFLOW_EXECUTORS.get(instance["workflow_type"], (None, None))
    try:
        return wf_mod.decide(instance_id, actor=user, decision=body.decision, comment=body.comment,
                              on_approved=on_approved, on_rejected=on_rejected)
    except wf_mod.WorkflowError as e:
        raise _wf_error(e)


# ---------------------------------------------------------------------------
# (e) REST API over the Gold layer, for downstream system consumption
# ---------------------------------------------------------------------------

# Shared by _CUSTOMERS_ENRICHED_CTE (display) and _golden_id_flagged_for_match_review
# (write-path block) so the two can never disagree about which golden_ids are
# currently sitting in a pending Match Review pair -- same "can't drift" principle
# as pending_edits below. A pair counts using the same live-override-overlay
# logic the Match Review queue endpoints themselves use (COALESCE the override
# onto the dbt-built snapshot's own review_status), and stays flagged through
# both 'pending' (not yet actioned by a steward) and 'in_review' (a steward's
# confirm/reject has been submitted and is awaiting Data Owner + Admin sign-off
# in the 'match_review_confirmation' workflow -- i.e. sitting in the
# stewardship app's Approvals list). Stopping at 'pending' left a window where
# a golden record about to be merged/retired by an in-flight match decision
# still showed as freely editable in the portal.
_PENDING_MATCH_REVIEW_CTE = """
    pending_match_pairs AS (
        SELECT q.source_system_a, q.source_record_id_a, q.source_system_b, q.source_record_id_b
        FROM main_gold.gold_match_review_queue q
        LEFT JOIN stewardship.match_review_overrides o ON o.pair_id = q.pair_id
        WHERE COALESCE(o.status, q.review_status) IN ('pending', 'in_review')
    ),
    flagged_golden_ids AS (
        SELECT DISTINCT cw.golden_id
        FROM main_gold.gold_crosswalk cw
        JOIN pending_match_pairs p
          ON (cw.source_system = p.source_system_a AND cw.source_record_id = p.source_record_id_a)
          OR (cw.source_system = p.source_system_b AND cw.source_record_id = p.source_record_id_b)
    )
"""

_CUSTOMERS_ENRICHED_CTE = """
    WITH crosswalk_max AS (
        SELECT golden_id, MAX(match_confidence_score) AS max_confidence
        FROM main_gold.gold_crosswalk
        GROUP BY golden_id
    ),
    tier1_threshold AS (
        SELECT auto_merge_threshold AS v FROM bus_rules.matching_thresholds WHERE tier_id = 'MT001'
    ),
    pending_edits AS (
        SELECT DISTINCT entity_id AS golden_id
        FROM governance.workflow_instances
        WHERE workflow_type = 'gold_record_edit' AND status = 'pending'
    ),
    """ + _PENDING_MATCH_REVIEW_CTE + """,
    enriched AS (
        SELECT
            gc.*,
            CASE
                WHEN gc.source_system_count <= 1 THEN 'provisional'
                WHEN COALESCE(cm.max_confidence, 0) >= (SELECT v FROM tier1_threshold) THEN 'tier1'
                ELSE 'tier2'
            END AS confidence_tier,
            (pe.golden_id IS NOT NULL) AS pending_approval,
            (fg.golden_id IS NOT NULL) AS pending_match_review
        FROM main_gold.gold_customers gc
        LEFT JOIN crosswalk_max cm ON cm.golden_id = gc.golden_id
        LEFT JOIN pending_edits pe ON pe.golden_id = gc.golden_id
        LEFT JOIN flagged_golden_ids fg ON fg.golden_id = gc.golden_id
    )
"""


def _golden_id_flagged_for_match_review(golden_id: str) -> bool:
    """Used by update_customer to block edits -- separate from the enriched
    CTE (rather than switching update_customer's own fetch to it) so the
    gold_record_edit workflow payload's old_snapshot stays exactly the raw
    gold_customers columns it always has been."""
    df = run_query("""
        WITH """ + _PENDING_MATCH_REVIEW_CTE + """
        SELECT 1 FROM flagged_golden_ids WHERE golden_id = ?
    """, [golden_id])
    return not df.empty


def _golden_id_has_pending_edit(golden_id: str) -> bool:
    """Used by update_customer to block a second concurrent gold_record_edit
    submission on a golden_id that already has one pending -- same 'can't
    drift' reasoning as _golden_id_flagged_for_match_review: the portal
    already renders this record read-only (pending_approval flag in
    _CUSTOMERS_ENRICHED_CTE) while a decision is in flight, so the API must
    refuse it too rather than relying on the UI alone."""
    df = run_query("""
        SELECT 1 FROM governance.workflow_instances
        WHERE workflow_type = 'gold_record_edit' AND status = 'pending' AND entity_id = ?
    """, [golden_id])
    return not df.empty


@app.get("/api/v1/customers")
def list_customers(
    limit: int = 10, offset: int = 0,
    state_code: Optional[str] = None,
    country_code: Optional[str] = None,
    source_system_count: Optional[int] = None,
    survivor_source_system: Optional[str] = None,
    confidence_tier: Optional[str] = None,
    pending_approval: Optional[bool] = None,
    q: Optional[str] = None,
    user: dict = Depends(auth_mod.require_gold_read),
):
    """Backs both the plain Browse Customers screen and every dashboard
    drill-through. Every filter composes against the same 'enriched' view
    (gold_customers + a live-computed confidence_tier + a pending_approval
    flag sourced from governance.workflow_instances), so a dashboard chart's
    count and its drill-through list can never disagree."""
    clauses, params = [], []
    if state_code:
        clauses.append("state_code = ?"); params.append(state_code)
    if country_code:
        clauses.append("country_code = ?"); params.append(country_code)
    if source_system_count is not None:
        clauses.append("source_system_count = ?"); params.append(source_system_count)
    if survivor_source_system:
        clauses.append("survivor_source_system = ?"); params.append(survivor_source_system)
    if confidence_tier:
        if confidence_tier not in ("provisional", "tier1", "tier2"):
            raise HTTPException(400, "confidence_tier must be one of: provisional, tier1, tier2")
        clauses.append("confidence_tier = ?"); params.append(confidence_tier)
    if pending_approval is not None:
        clauses.append("pending_approval = ?"); params.append(pending_approval)
    if q:
        clauses.append(
            "(lower(first_name) LIKE lower(?) OR lower(last_name) LIKE lower(?) "
            "OR lower(email) LIKE lower(?) OR golden_id LIKE ?)"
        )
        like = f"%{q}%"
        params.extend([like, like, like, f"%{q}%"])

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    df = run_query(
        _CUSTOMERS_ENRICHED_CTE + f"SELECT * FROM enriched {where} ORDER BY golden_id LIMIT ? OFFSET ?",
        params + [limit, offset],
    )
    total = run_query(
        _CUSTOMERS_ENRICHED_CTE + f"SELECT COUNT(*) AS n FROM enriched {where}", params
    ).iloc[0]["n"]
    return {"total": int(total), "limit": limit, "offset": offset, "customers": to_records(df)}


@app.get("/api/v1/customers/{golden_id}")
def get_customer(golden_id: str, user: dict = Depends(auth_mod.require_gold_read)):
    df = run_query(_CUSTOMERS_ENRICHED_CTE + "SELECT * FROM enriched WHERE golden_id = ?", [golden_id])
    if df.empty:
        raise HTTPException(404, f"No golden record found for {golden_id}")
    return to_records(df)[0]


@app.get("/api/v1/customers/{golden_id}/sources")
def get_customer_sources(golden_id: str, user: dict = Depends(auth_mod.require_gold_read)):
    df = run_query("SELECT * FROM main_gold.gold_crosswalk WHERE golden_id = ?", [golden_id])
    if df.empty:
        raise HTTPException(404, f"No crosswalk entries found for {golden_id}")
    return {"golden_id": golden_id, "sources": to_records(df)}


@app.get("/api/v1/customers/{golden_id}/audit-trail")
def get_customer_audit_trail(golden_id: str, limit: int = 10, offset: int = 0,
                              user: dict = Depends(auth_mod.require_gold_read)):
    """Read-only, append-only history of every creation, edit (manual or
    systemic), and logical delete recorded for one golden record. Gated the
    same way as viewing the record itself (any gold_access of 'read' or
    'read_write') -- there is no corresponding write/delete endpoint, by design.
    Falls back to a reconstruction from the audit log itself if the record has
    been logically deleted (no longer present in main_gold.gold_customers).

    Pagination happens at the logical-event level (one change_batch_id --
    e.g. a single edit touching 5 fields -- is one page item), not the raw
    field-diff row level, so a page never splits one event's field rows
    across two pages."""
    events = audit_mod.get_audit_trail(golden_id)
    if not events:
        raise HTTPException(404, f"No audit history found for {golden_id}")

    current_df = run_query("SELECT * FROM main_gold.gold_customers WHERE golden_id = ?", [golden_id])
    if not current_df.empty:
        current = to_records(current_df)[0]
        is_active = True
    else:
        current = audit_mod.reconstruct_last_known_state(golden_id)
        is_active = False

    batches = {}
    order = []
    for e in events:
        bid = e["change_batch_id"]
        if bid not in batches:
            batches[bid] = []
            order.append(bid)
        batches[bid].append(e)
    batch_list = sorted(batches.values(), key=lambda b: b[0]["event_ts"], reverse=True)
    total = len(batch_list)
    page_batches = batch_list[offset:offset + limit]
    page_events = [e for batch in page_batches for e in batch]

    return {
        "golden_id": golden_id, "is_active": is_active, "current": current,
        "total": total, "limit": limit, "offset": offset, "events": page_events,
    }


class CustomerUpdateRequest(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    address_line1: Optional[str] = None
    address_line2: Optional[str] = None
    city: Optional[str] = None
    state_code: Optional[str] = None
    postal_code: Optional[str] = None
    country_code: Optional[str] = None


@app.put("/api/v1/customers/{golden_id}")
def update_customer(golden_id: str, body: CustomerUpdateRequest, user: dict = Depends(auth_mod.require_gold_write)):
    """Submits a gold-record edit into the maker-checker workflow instead of
    applying it directly. It only takes effect once 2 different Data Owners
    (neither of them this maker) have both approved -- see
    _execute_gold_edit below and the 'gold_record_edit' workflow_type
    (governance.workflow_definitions: 1 step, approvals_required=2)."""
    df = run_query("SELECT * FROM main_gold.gold_customers WHERE golden_id = ?", [golden_id])
    if df.empty:
        raise HTTPException(404, f"No golden record found for {golden_id}")

    if _golden_id_flagged_for_match_review(golden_id):
        raise HTTPException(
            409,
            f"{golden_id} is currently in the Match Review queue as a likely duplicate of "
            "another golden record and can't be edited until a data steward resolves it.",
        )

    if _golden_id_has_pending_edit(golden_id):
        raise HTTPException(
            409,
            f"{golden_id} already has an edit pending approval and can't be edited again "
            "until that workflow is decided.",
        )

    updates = {k: v for k, v in body.dict().items() if v is not None}
    if not updates:
        return {"golden_id": golden_id, "status": "no_changes"}

    try:
        instance = wf_mod.start_workflow(
            "gold_record_edit", entity_type="gold_customer", entity_id=golden_id, maker=user,
            action_type="update", payload={"updates": updates, "old_snapshot": to_records(df)[0]},
        )
    except wf_mod.WorkflowError as e:
        raise _wf_error(e)

    return {
        "golden_id": golden_id,
        "status": "pending_approval",
        "workflow_instance_id": instance["instance_id"],
        "steps": instance["steps"],
    }


def _execute_gold_edit(instance: dict, actor: dict) -> dict:
    """on_approved callback for 'gold_record_edit'. Applies the update the
    maker proposed, attributing the audit-trail entry to the maker (not the
    approvers) -- the maker made the edit; the Data Owners authorized it."""
    golden_id = instance["entity_id"]
    updates = instance["payload"]["updates"]
    old_snapshot = instance["payload"]["old_snapshot"]

    set_clause = ", ".join(f"{col} = ?" for col in updates)
    run_write(f"UPDATE main_gold.gold_customers SET {set_clause} WHERE golden_id = ?",
              list(updates.values()) + [golden_id])
    updated = run_query("SELECT * FROM main_gold.gold_customers WHERE golden_id = ?", [golden_id])
    if updated.empty:
        return {"error": f"golden_id {golden_id} no longer exists (logically deleted since submission)"}

    audit_mod.log_update(
        golden_id, old_record=old_snapshot, new_record=to_records(updated)[0],
        event_source="portal_manual_edit",
        changed_by=instance["maker_user_id"], changed_by_label=instance["maker_label"],
        change_reason="Manual edit via MDM Data Hub Portal, approved by 2 Data Owners",
    )
    return {"action": "applied", "golden_id": golden_id, "updated_record": to_records(updated)[0]}


# ---------------------------------------------------------------------------
# (c.1) Customer Portal Dashboard -- aggregate summary for the portal's
# "Dashboard" landing screen. One pane (unlike Stewardship's two): every
# breakdown is computed from the same 'enriched' gold_customers view used by
# GET /api/v1/customers's filters above, so a dashboard chart's count and its
# Browse Customers drill-through can never disagree. There's no per-user
# "your activity" section here (gold_customers isn't a per-steward queue).
# ---------------------------------------------------------------------------

_GEOGRAPHY_TOP_N = 12


@app.get("/api/v1/portal/dashboard/customers-summary")
def customers_dashboard_summary(user: dict = Depends(auth_mod.require_gold_read)):
    total = int(run_query("SELECT COUNT(*) AS n FROM main_gold.gold_customers").iloc[0]["n"])

    source_count_df = run_query(
        _CUSTOMERS_ENRICHED_CTE +
        "SELECT source_system_count, COUNT(*) AS n FROM enriched GROUP BY 1 ORDER BY 1"
    )
    tier_df = run_query(
        _CUSTOMERS_ENRICHED_CTE +
        "SELECT confidence_tier, COUNT(*) AS n FROM enriched GROUP BY 1"
    )
    survivor_df = run_query(
        "SELECT survivor_source_system, COUNT(*) AS n FROM main_gold.gold_customers GROUP BY 1 ORDER BY 1"
    )
    pending_df = run_query(
        _CUSTOMERS_ENRICHED_CTE +
        "SELECT pending_approval, COUNT(*) AS n FROM enriched GROUP BY 1"
    )

    def top_n_with_other(df, col):
        df = df.sort_values("n", ascending=False)
        head = df.iloc[:_GEOGRAPHY_TOP_N]
        rest = df.iloc[_GEOGRAPHY_TOP_N:]
        rows = [{"label": (r[col] or "(unknown)"), "count": int(r["n"])} for _, r in head.iterrows()]
        if len(rest):
            rows.append({"label": "Other", "count": int(rest["n"].sum())})
        return rows

    state_df = run_query("""
        SELECT COALESCE(state_code, '(unknown)') AS state_code, COUNT(*) AS n
        FROM main_gold.gold_customers GROUP BY 1
    """)
    country_df = run_query("""
        SELECT COALESCE(country_code, '(unknown)') AS country_code, COUNT(*) AS n
        FROM main_gold.gold_customers GROUP BY 1
    """)

    trend_df = run_query("""
        SELECT CAST(event_ts AS DATE) AS d, event_type, COUNT(*) AS n
        FROM audit.audit_trail
        WHERE event_type IN ('created', 'updated', 'logically_deleted')
        GROUP BY 1, 2
        ORDER BY 1
    """)
    trend = {}
    for r in trend_df.itertuples():
        d = str(r.d)
        trend.setdefault(d, {"date": d, "created": 0, "updated": 0, "logically_deleted": 0})
        trend[d][r.event_type] += int(r.n)

    tier_counts = {r.confidence_tier: int(r.n) for r in tier_df.itertuples()}
    pending_counts = {bool(r.pending_approval): int(r.n) for r in pending_df.itertuples()}

    return {
        "total": total,
        "by_source_count": [{"source_system_count": int(r.source_system_count), "count": int(r.n)} for r in source_count_df.itertuples()],
        "by_confidence_tier": [
            {"tier": "provisional", "count": tier_counts.get("provisional", 0)},
            {"tier": "tier1", "count": tier_counts.get("tier1", 0)},
            {"tier": "tier2", "count": tier_counts.get("tier2", 0)},
        ],
        "by_survivor_source": [{"source_system": r.survivor_source_system, "count": int(r.n)} for r in survivor_df.itertuples()],
        "by_state": top_n_with_other(state_df, "state_code"),
        "by_country": top_n_with_other(country_df, "country_code"),
        "pending_approval": {
            "pending": pending_counts.get(True, 0),
            "not_pending": pending_counts.get(False, 0),
        },
        "trend": sorted(trend.values(), key=lambda x: x["date"]),
    }


# ---------------------------------------------------------------------------
# (d) Lineage and impact analysis
# ---------------------------------------------------------------------------

@app.get("/api/v1/lineage/impact")
def lineage_impact(layer: str, table: str, column: str, user: dict = Depends(auth_mod.get_current_user)):
    """Forward trace: what downstream tables/columns are affected by this one?"""
    return {"origin": f"{layer}.{table}.{column}", "downstream": lineage_mod.impact_analysis(layer, table, column)}


@app.get("/api/v1/lineage/trace")
def lineage_trace(layer: str, table: str, column: str, user: dict = Depends(auth_mod.get_current_user)):
    """Backward trace: where did this column's data come from?"""
    return {"target": f"{layer}.{table}.{column}", "upstream": lineage_mod.lineage_trace(layer, table, column)}


@app.get("/api/v1/lineage/record/{golden_id}")
def lineage_for_record(golden_id: str, user: dict = Depends(auth_mod.get_current_user)):
    """Full lineage for one golden record: every contributing source record."""
    result = lineage_mod.trace_golden_record(golden_id)
    if result is None:
        raise HTTPException(404, f"No golden record found for {golden_id}")
    return result


@app.get("/api/v1/lineage/graph")
def lineage_graph(user: dict = Depends(auth_mod.get_current_user)):
    """Full lineage metadata graph (all edges) for the Data Governance network diagram.
    Available to any authenticated user regardless of gold_access -- this is
    governance/metadata, not customer data."""
    df = run_query("SELECT from_layer, from_table, from_column, to_layer, to_table, to_column, "
                    "transform_rule_id, transform_description FROM main_rules.lineage_edges")
    edges = [
        {
            "from": f"{r.from_layer}.{r.from_table}.{r.from_column}",
            "to": f"{r.to_layer}.{r.to_table}.{r.to_column}",
            "rule_id": r.transform_rule_id,
            "description": r.transform_description,
        }
        for r in df.itertuples()
    ]
    return {"edges": edges}


# ---------------------------------------------------------------------------
# (c) Data Stewardship app backend, with AI-assisted remediation
# ---------------------------------------------------------------------------

_ACTIVITY_TO_WORKFLOW_STATUS = {
    "submitted": None,      # any outcome -- just "did I submit something for this entity"
    "approved": "approved",
    "pending": "pending",
    "sent_back": "rejected",  # an approver rejected the maker's proposal, returning it to the shared queue
}

_EXCEPTIONS_BASE_SELECT = """
    SELECT eq.*,
           COALESCE(o.remediation_status, eq.remediation_status) AS effective_status
    FROM main_silver.exceptions_queue eq
    LEFT JOIN stewardship.exception_status_overrides o
      ON o.exception_id = eq.exception_id
"""


def _paginate_df(df, limit: int, offset: int) -> dict:
    """Shared pagination envelope for endpoints that build their full,
    filtered result set as a DataFrame first (status/reason/activity
    branches all already do this) -- total is the full filtered count,
    items is just the requested page."""
    total = len(df)
    page = df.iloc[offset:offset + limit]
    return {"total": total, "count": total, "limit": limit, "offset": offset, "items": to_records(page)}


@app.get("/api/v1/stewardship/queue")
def get_queue(
    status: str = "open",
    reason: Optional[str] = None,
    activity: Optional[str] = None,
    limit: int = 10,
    offset: int = 0,
    user: dict = Depends(auth_mod.require_steward_or_owner),
):
    """Backs both the plain Exception Queue tab (status=open by default) and
    every dashboard drill-through: a single status (incl. 'all' for every
    status), a rejection reason (list_contains against reject_reasons,
    deliberately ignoring status -- the dashboard's by-reason chart counts
    are computed across the whole queue, not just open items), or an
    'activity' slice scoped to the logged-in user's own maker-checker
    submissions (governance.workflow_instances.maker_user_id). limit/offset
    paginate whichever of these result sets was selected; 'count'/'total'
    in the response always reflect the full filtered set, not just the page."""
    if activity:
        if activity not in _ACTIVITY_TO_WORKFLOW_STATUS:
            raise HTTPException(400, f"Unknown activity '{activity}'")
        wf_status = _ACTIVITY_TO_WORKFLOW_STATUS[activity]
        if wf_status is None:
            df = run_query(_EXCEPTIONS_BASE_SELECT + """
                WHERE eq.exception_id IN (
                    SELECT entity_id FROM governance.workflow_instances
                    WHERE workflow_type = 'stewardship_remediation' AND maker_user_id = ?
                )
                ORDER BY eq.queued_ts
            """, [user["user_id"]])
        else:
            df = run_query(_EXCEPTIONS_BASE_SELECT + """
                WHERE eq.exception_id IN (
                    SELECT entity_id FROM governance.workflow_instances
                    WHERE workflow_type = 'stewardship_remediation' AND maker_user_id = ? AND status = ?
                )
                ORDER BY eq.queued_ts
            """, [user["user_id"], wf_status])
        return {"status_filter": None, "reason_filter": None, "activity_filter": activity,
                **_paginate_df(df, limit, offset)}

    if reason:
        df = run_query(_EXCEPTIONS_BASE_SELECT + """
            WHERE list_contains(eq.reject_reasons, ?)
            ORDER BY eq.queued_ts
        """, [reason])
        return {"status_filter": None, "reason_filter": reason, "activity_filter": None,
                **_paginate_df(df, limit, offset)}

    if status == "all":
        df = run_query(_EXCEPTIONS_BASE_SELECT + " ORDER BY eq.queued_ts")
    else:
        df = run_query(_EXCEPTIONS_BASE_SELECT + """
            WHERE COALESCE(o.remediation_status, eq.remediation_status) = ?
            ORDER BY eq.queued_ts
        """, [status])
    return {"status_filter": status, "reason_filter": None, "activity_filter": None,
            **_paginate_df(df, limit, offset)}


@app.get("/api/v1/stewardship/queue/{exception_id}")
def get_queue_item(exception_id: str, user: dict = Depends(auth_mod.require_steward_or_owner)):
    df = run_query(_EXCEPTIONS_BASE_SELECT + " WHERE eq.exception_id = ?", [exception_id])
    if df.empty:
        raise HTTPException(404, "Exception not found")
    return to_records(df)[0]


@app.post("/api/v1/stewardship/queue/{exception_id}/suggest")
def suggest(exception_id: str, user: dict = Depends(auth_mod.require_steward_or_owner)):
    df = run_query("SELECT * FROM main_silver.exceptions_queue WHERE exception_id = ?", [exception_id])
    if df.empty:
        raise HTTPException(404, "Exception not found")
    record = to_records(df)[0]
    reject_reasons = list(record.get("reject_reasons") or [])

    suggestion = suggest_remediation(record, reject_reasons)

    run_write("""
        INSERT INTO stewardship.remediation_log
            (log_id, exception_id, action, suggested_fields, rationale, suggestion_source)
        VALUES (?, ?, 'ai_suggested', ?, ?, ?)
    """, [str(uuid.uuid4()), exception_id, json.dumps(suggestion["suggested_fields"]),
          suggestion["rationale"], suggestion["source"]])

    return suggestion


class ResolveRequest(BaseModel):
    applied_fields: dict
    steward_note: Optional[str] = None


@app.post("/api/v1/stewardship/queue/{exception_id}/resolve")
def resolve(exception_id: str, body: ResolveRequest, user: dict = Depends(auth_mod.require_steward_or_owner)):
    """Re-validates and, if the corrected record now passes, submits it into
    the 'stewardship_remediation' maker-checker workflow (1 Data Owner,
    different from whoever resolved this) instead of reprocessing it
    immediately. The actual silver upsert + match/merge only happens once
    that Data Owner approves -- see _execute_stewardship_resolve below."""
    df = run_query("SELECT * FROM main_silver.exceptions_queue WHERE exception_id = ?", [exception_id])
    if df.empty:
        raise HTTPException(404, "Exception not found")
    record = to_records(df)[0]
    merged = {**record, **body.applied_fields}

    # Re-check the corrected record against the same reject-severity rules that
    # routed it here in the first place (column_rules.csv). Only a record that
    # now genuinely passes validation is allowed to be submitted for approval --
    # otherwise it stays in the queue (still 'open') and the steward is told
    # exactly what's still failing, instead of silently trusting the click.
    still_failing = validate_record(merged)
    if still_failing:
        run_write("""
            INSERT INTO stewardship.remediation_log
                (log_id, exception_id, action, applied_fields, rationale, steward_note, suggestion_source)
            VALUES (?, ?, 'steward_resolve_blocked', ?, ?, ?, 'steward')
        """, [str(uuid.uuid4()), exception_id, json.dumps(body.applied_fields),
              "Still failing: " + "; ".join(still_failing), body.steward_note])
        raise HTTPException(422, detail={
            "message": "This record still fails validation and was not resolved. "
                       "It remains in the exception queue.",
            "still_failing": still_failing,
        })

    run_write("""
        INSERT INTO stewardship.remediation_log
            (log_id, exception_id, action, applied_fields, steward_note, suggestion_source)
        VALUES (?, ?, 'steward_resolve_submitted', ?, ?, 'steward')
    """, [str(uuid.uuid4()), exception_id, json.dumps(body.applied_fields), body.steward_note])

    run_write("""
        INSERT OR REPLACE INTO stewardship.exception_status_overrides (exception_id, remediation_status)
        VALUES (?, 'in_review')
    """, [exception_id])

    try:
        instance = wf_mod.start_workflow(
            "stewardship_remediation", entity_type="exception", entity_id=exception_id, maker=user,
            action_type="resolve", payload={"merged": merged, "applied_fields": body.applied_fields},
        )
    except wf_mod.WorkflowError as e:
        raise _wf_error(e)

    return {
        "exception_id": exception_id,
        "status": "pending_approval",
        "applied_fields": body.applied_fields,
        "workflow_instance_id": instance["instance_id"],
        "steps": instance["steps"],
    }


def _execute_stewardship_resolve(instance: dict, actor: dict) -> dict:
    """on_approved callback for 'stewardship_remediation' (action_type='resolve').
    Everything resolve() used to do immediately after validation, now deferred
    to here. Attributes the resulting reprocessing/audit entries to the
    original maker (the steward who actually made the correction), not the
    Data Owner who approved it."""
    exception_id = instance["entity_id"]
    merged = instance["payload"]["merged"]
    applied_fields = instance["payload"]["applied_fields"]
    maker = {"user_id": instance["maker_user_id"], "full_name": instance["maker_label"]}

    run_write("""
        INSERT OR REPLACE INTO stewardship.remediated_records
            (exception_id, source_system, source_record_id, first_name, last_name, email, phone,
             address_line1, address_line2, city, state_code, postal_code, country_code)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [exception_id, merged["source_system"], merged["source_record_id"],
          merged.get("first_name"), merged.get("last_name"), merged.get("email"), merged.get("phone"),
          merged.get("address_line1"), merged.get("address_line2"), merged.get("city"),
          merged.get("state_code"), merged.get("postal_code"), merged.get("country_code")])

    run_write("""
        INSERT INTO stewardship.remediation_log
            (log_id, exception_id, action, applied_fields, steward_note, suggestion_source)
        VALUES (?, ?, 'steward_resolved', ?, 'Approved by Data Owner', 'steward')
    """, [str(uuid.uuid4()), exception_id, json.dumps(applied_fields)])

    run_write("""
        INSERT OR REPLACE INTO stewardship.exception_status_overrides (exception_id, remediation_status)
        VALUES (?, 'resolved')
    """, [exception_id])

    reprocess_result = reprocess_corrected_record(merged, actor=maker)
    return {"action": "resolved", "reprocessing": reprocess_result}


def _rollback_stewardship_submission(instance: dict, actor: dict) -> dict:
    """on_rejected callback shared by both 'resolve' and 'reject' submissions
    to 'stewardship_remediation': a Data Owner rejecting the steward's
    proposed action puts the exception back in the open queue so it can be
    reworked, rather than silently disappearing."""
    exception_id = instance["entity_id"]
    run_write("""
        INSERT INTO stewardship.remediation_log
            (log_id, exception_id, action, steward_note, suggestion_source)
        VALUES (?, ?, 'approval_rejected', ?, 'steward')
    """, [str(uuid.uuid4()), exception_id, f"Rejected by Data Owner: {actor.get('full_name', 'unknown')}"])
    run_write("""
        INSERT OR REPLACE INTO stewardship.exception_status_overrides (exception_id, remediation_status)
        VALUES (?, 'open')
    """, [exception_id])
    return {"action": "returned_to_open_queue"}


class RejectRequest(BaseModel):
    steward_note: Optional[str] = None


@app.post("/api/v1/stewardship/queue/{exception_id}/reject")
def reject(exception_id: str, body: RejectRequest, user: dict = Depends(auth_mod.require_steward_or_owner)):
    """Submits a proposal to permanently reject this exception into the same
    'stewardship_remediation' workflow as resolve() (action_type='reject')
    -- a Data Owner has to sign off on excluding a record from the pipeline
    too, not just on correcting one."""
    df = run_query("SELECT * FROM main_silver.exceptions_queue WHERE exception_id = ?", [exception_id])
    if df.empty:
        raise HTTPException(404, "Exception not found")

    run_write("""
        INSERT INTO stewardship.remediation_log
            (log_id, exception_id, action, steward_note, suggestion_source)
        VALUES (?, ?, 'steward_reject_submitted', ?, 'steward')
    """, [str(uuid.uuid4()), exception_id, body.steward_note])

    run_write("""
        INSERT OR REPLACE INTO stewardship.exception_status_overrides (exception_id, remediation_status)
        VALUES (?, 'in_review')
    """, [exception_id])

    try:
        instance = wf_mod.start_workflow(
            "stewardship_remediation", entity_type="exception", entity_id=exception_id, maker=user,
            action_type="reject", payload={"steward_note": body.steward_note},
        )
    except wf_mod.WorkflowError as e:
        raise _wf_error(e)

    return {
        "exception_id": exception_id,
        "status": "pending_approval",
        "workflow_instance_id": instance["instance_id"],
        "steps": instance["steps"],
    }


def _execute_stewardship_reject(instance: dict, actor: dict) -> dict:
    """on_approved callback for 'stewardship_remediation' (action_type='reject')."""
    exception_id = instance["entity_id"]
    steward_note = instance["payload"].get("steward_note")
    run_write("""
        INSERT INTO stewardship.remediation_log
            (log_id, exception_id, action, steward_note, suggestion_source)
        VALUES (?, ?, 'steward_rejected', ?, 'steward')
    """, [str(uuid.uuid4()), exception_id, steward_note])
    run_write("""
        INSERT OR REPLACE INTO stewardship.exception_status_overrides (exception_id, remediation_status)
        VALUES (?, 'rejected')
    """, [exception_id])
    return {"action": "rejected"}


def _dispatch_stewardship_remediation(instance: dict, actor: dict) -> dict:
    """The 'stewardship_remediation' workflow_type covers two different
    maker actions (resolve vs. reject) sharing one approval chain; this picks
    the right on_approved executor based on the instance's action_type."""
    if instance["action_type"] == "resolve":
        return _execute_stewardship_resolve(instance, actor)
    elif instance["action_type"] == "reject":
        return _execute_stewardship_reject(instance, actor)
    raise ValueError(f"Unknown action_type '{instance['action_type']}' for stewardship_remediation")


# ---------------------------------------------------------------------------
# (d) Match Review queue -- borderline fuzzy-match pairs from the gold layer's
# embedding-similarity tier (scripts/generate_matches.py) that were not
# auto-merged. A data steward confirms ("same customer, merge them") or
# rejects ("coincidence, keep separate"). Same role gating as the exception
# queue: dataSteward/dataOwner only, admin deliberately excluded.
#
# A confirm decision, once approved through the full maker-checker chain,
# now auto-processes in real time (reprocessing.reprocess_confirmed_match_review):
# attribute-level survivorship is re-run across the full group of source
# records already contributing to either side's golden record, the smaller
# of the two golden_ids survives, and the other is retired (deleted from
# main_gold.gold_customers, logged to the audit trail as 'logically_deleted')
# rather than waiting for the next full pipeline rebuild. A reject decision
# still has nothing to reprocess -- rejecting never changes gold, in real
# time or in batch, it only excludes the pair from future matching runs.
# ---------------------------------------------------------------------------

_MATCH_REVIEW_BASE_SELECT = """
    SELECT q.*,
           COALESCE(o.status, q.review_status) AS effective_status,
           o.steward_note AS status_note
    FROM main_gold.gold_match_review_queue q
    LEFT JOIN stewardship.match_review_overrides o
      ON o.pair_id = q.pair_id
"""


def _match_similarity_bucket_bounds():
    """Live Tier 2 fuzzy-match thresholds (bus_rules.matching_thresholds is
    editable at runtime via the Rules Configuration workflow), split into 3
    equal-width buckets between review_lower_threshold and auto_merge_threshold.
    Shared by the dashboard summary endpoint and the similarity_bucket filter
    below so the two never drift apart."""
    thr_df = run_query("""
        SELECT review_lower_threshold, auto_merge_threshold
        FROM bus_rules.matching_thresholds
        WHERE tier_id = 'MT002'
    """)
    if thr_df.empty:
        low, high = 0.35, 0.80
    else:
        low = float(thr_df.iloc[0]["review_lower_threshold"])
        high = float(thr_df.iloc[0]["auto_merge_threshold"])
    third = (high - low) / 3 if high > low else 0.15
    return low, high, third


@app.get("/api/v1/stewardship/match-review")
def get_match_review_queue(
    status: str = "pending",
    similarity_bucket: Optional[int] = None,
    activity: Optional[str] = None,
    limit: int = 10,
    offset: int = 0,
    user: dict = Depends(auth_mod.require_steward_or_owner),
):
    """Backs both the plain Match Review tab (status=pending by default) and
    every dashboard drill-through: a single status (incl. 'all'), a
    similarity-score bucket (0/1/2, deliberately ignoring status -- the
    dashboard's bucket chart counts every pair regardless of status), or an
    'activity' slice scoped to the logged-in user's own maker-checker
    submissions. limit/offset paginate whichever of these result sets was
    selected; 'count'/'total' in the response always reflect the full
    filtered set, not just the page."""
    if activity:
        if activity not in _ACTIVITY_TO_WORKFLOW_STATUS:
            raise HTTPException(400, f"Unknown activity '{activity}'")
        wf_status = _ACTIVITY_TO_WORKFLOW_STATUS[activity]
        if wf_status is None:
            df = run_query(_MATCH_REVIEW_BASE_SELECT + """
                WHERE q.pair_id IN (
                    SELECT entity_id FROM governance.workflow_instances
                    WHERE workflow_type = 'match_review_confirmation' AND maker_user_id = ?
                )
                ORDER BY q.queued_ts
            """, [user["user_id"]])
        else:
            df = run_query(_MATCH_REVIEW_BASE_SELECT + """
                WHERE q.pair_id IN (
                    SELECT entity_id FROM governance.workflow_instances
                    WHERE workflow_type = 'match_review_confirmation' AND maker_user_id = ? AND status = ?
                )
                ORDER BY q.queued_ts
            """, [user["user_id"], wf_status])
        return {"status_filter": None, "bucket_filter": None, "activity_filter": activity,
                **_paginate_df(df, limit, offset)}

    if similarity_bucket is not None:
        if similarity_bucket not in (0, 1, 2):
            raise HTTPException(400, "similarity_bucket must be 0, 1, or 2")
        low, high, third = _match_similarity_bucket_bounds()
        bounds = [(low, low + third), (low + third, low + 2 * third), (low + 2 * third, high)]
        lo, hi = bounds[similarity_bucket]
        if similarity_bucket == 2:
            df = run_query(_MATCH_REVIEW_BASE_SELECT + """
                WHERE q.similarity_score >= ? AND q.similarity_score <= ?
                ORDER BY q.queued_ts
            """, [lo, hi])
        else:
            df = run_query(_MATCH_REVIEW_BASE_SELECT + """
                WHERE q.similarity_score >= ? AND q.similarity_score < ?
                ORDER BY q.queued_ts
            """, [lo, hi])
        return {"status_filter": None, "bucket_filter": similarity_bucket, "activity_filter": None,
                **_paginate_df(df, limit, offset)}

    if status == "all":
        df = run_query(_MATCH_REVIEW_BASE_SELECT + " ORDER BY q.queued_ts")
    else:
        df = run_query(_MATCH_REVIEW_BASE_SELECT + """
            WHERE COALESCE(o.status, q.review_status) = ?
            ORDER BY q.queued_ts
        """, [status])
    return {"status_filter": status, "bucket_filter": None, "activity_filter": None,
            **_paginate_df(df, limit, offset)}


@app.get("/api/v1/stewardship/match-review/{pair_id}")
def get_match_review_item(pair_id: str, user: dict = Depends(auth_mod.require_steward_or_owner)):
    df = run_query(_MATCH_REVIEW_BASE_SELECT + " WHERE q.pair_id = ?", [pair_id])
    if df.empty:
        raise HTTPException(404, "Match review candidate not found")
    return to_records(df)[0]


class MatchReviewDecisionRequest(BaseModel):
    steward_note: Optional[str] = None


@app.post("/api/v1/stewardship/match-review/{pair_id}/confirm")
def confirm_match(pair_id: str, body: MatchReviewDecisionRequest, user: dict = Depends(auth_mod.require_steward_or_owner)):
    """Submits a confirm decision into the 'match_review_confirmation'
    maker-checker workflow (Data Owner, then Admin -- 2 sequential levels)
    instead of recording it immediately. See _execute_match_review below."""
    return _submit_match_review_decision(pair_id, "confirm", body, user)


@app.post("/api/v1/stewardship/match-review/{pair_id}/reject")
def reject_match(pair_id: str, body: MatchReviewDecisionRequest, user: dict = Depends(auth_mod.require_steward_or_owner)):
    """Submits a reject decision into the same 'match_review_confirmation'
    workflow as confirm() -- deciding these pairs are NOT duplicates still
    needs the same 2-level sign-off, not just a steward's own judgment."""
    return _submit_match_review_decision(pair_id, "reject", body, user)


def _submit_match_review_decision(pair_id: str, action_type: str, body: MatchReviewDecisionRequest, user: dict):
    df = run_query("SELECT * FROM main_gold.gold_match_review_queue WHERE pair_id = ?", [pair_id])
    if df.empty:
        raise HTTPException(404, "Match review candidate not found")

    run_write("""
        INSERT OR REPLACE INTO stewardship.match_review_overrides (pair_id, status, steward_note)
        VALUES (?, 'in_review', ?)
    """, [pair_id, body.steward_note])
    run_write("""
        INSERT INTO stewardship.match_review_log (log_id, pair_id, action, steward_note)
        VALUES (?, ?, ?, ?)
    """, [str(uuid.uuid4()), pair_id, f"{action_type}_submitted", body.steward_note])

    try:
        instance = wf_mod.start_workflow(
            "match_review_confirmation", entity_type="match_pair", entity_id=pair_id, maker=user,
            action_type=action_type, payload={"steward_note": body.steward_note},
        )
    except wf_mod.WorkflowError as e:
        raise _wf_error(e)

    return {
        "pair_id": pair_id,
        "status": "pending_approval",
        "workflow_instance_id": instance["instance_id"],
        "steps": instance["steps"],
        "note": "Submitted for approval (Data Owner, then Admin sign-off) before this decision takes effect.",
    }


def _execute_match_review(instance: dict, actor: dict) -> dict:
    """on_approved callback for 'match_review_confirmation' -- runs once both
    levels (Data Owner, then Admin) have signed off. Applies whichever
    decision (confirm/reject) the steward originally proposed.

    The stewardship record is marked complete (status written to
    stewardship.match_review_overrides) BEFORE the real-time merge is
    attempted for a 'confirmed' decision, not after -- the human decision
    stands regardless of whether the mechanical merge step itself succeeds
    (same posture as every other on_approved callback in this app: see
    workflow_engine.decide()'s executor_error handling). A merge failure
    therefore surfaces in the workflow instance's result rather than
    silently leaving the pair undecided; the next full
    `generate_matches.py` run would still pick up the 'confirmed' override
    and realize the merge as a fallback."""
    pair_id = instance["entity_id"]
    action_type = instance["action_type"]
    steward_note = instance["payload"].get("steward_note")
    final_status = "confirmed" if action_type == "confirm" else "rejected"

    run_write("""
        INSERT OR REPLACE INTO stewardship.match_review_overrides (pair_id, status, steward_note)
        VALUES (?, ?, ?)
    """, [pair_id, final_status, steward_note])
    run_write("""
        INSERT INTO stewardship.match_review_log (log_id, pair_id, action, steward_note)
        VALUES (?, ?, ?, ?)
    """, [str(uuid.uuid4()), pair_id, final_status, steward_note])

    if final_status != "confirmed":
        return {
            "action": final_status,
            "pair_id": pair_id,
            "note": "This pair is permanently excluded from future matching runs and will not "
                    "resurface in the review queue.",
        }

    # Attribute the merge/audit trail to the original maker (the steward who
    # confirmed the match), not the Data Owner/Admin who approved it --
    # same convention as _execute_stewardship_resolve above.
    maker = {"user_id": instance["maker_user_id"], "full_name": instance["maker_label"]}
    merge_result = reprocess_confirmed_match_review(pair_id, actor=maker)

    if merge_result["action"] == "merged":
        note = (
            f"Merged in real time: {merge_result['retired_golden_id']} was retired into "
            f"{merge_result['golden_id']} (smaller golden_id survives), survivorship recomputed "
            f"across {merge_result['contributing_sources']} contributing source record(s)."
        )
    else:
        note = f"Both sides already belong to {merge_result['golden_id']} -- nothing further to merge."

    return {"action": final_status, "pair_id": pair_id, "note": note, "merge": merge_result}


def _rollback_match_review_submission(instance: dict, actor: dict) -> dict:
    """on_rejected callback: an approver rejecting the steward's proposed
    confirm/reject decision returns the pair to 'pending' so it resurfaces
    in the normal Match Review queue for a fresh look."""
    pair_id = instance["entity_id"]
    run_write("""
        INSERT OR REPLACE INTO stewardship.match_review_overrides (pair_id, status, steward_note)
        VALUES (?, 'pending', ?)
    """, [pair_id, f"Approval rejected by {actor.get('full_name', 'unknown')} ({actor.get('role')})"])
    run_write("""
        INSERT INTO stewardship.match_review_log (log_id, pair_id, action, steward_note)
        VALUES (?, ?, 'approval_rejected', ?)
    """, [str(uuid.uuid4()), pair_id, f"Rejected by {actor.get('full_name', 'unknown')} ({actor.get('role')})"])
    return {"action": "returned_to_pending_queue"}


# ---------------------------------------------------------------------------
# (d.1) Data Stewardship Dashboard -- aggregate summaries for the console's
# "Dashboard" landing tab. Two panes: Exceptions and Match Review. Each
# endpoint returns queue-wide numbers (the underlying queue is shared, not
# assigned to individual stewards) plus a "your_activity" slice scoped to the
# logged-in user, built from the only per-user attribution that exists for
# these entities: maker_user_id on the workflow instance they submitted
# (governance.workflow_instances), not the queue snapshot itself.
# ---------------------------------------------------------------------------

def _my_workflow_activity(workflow_type: str, user_id: str) -> dict:
    df = run_query("""
        SELECT status, COUNT(*) AS n
        FROM governance.workflow_instances
        WHERE workflow_type = ? AND maker_user_id = ?
        GROUP BY 1
    """, [workflow_type, user_id])
    mine = {"submitted": 0, "approved": 0, "pending": 0, "sent_back": 0}
    for r in df.itertuples():
        n = int(r.n)
        mine["submitted"] += n
        if r.status == "approved":
            mine["approved"] += n
        elif r.status == "pending":
            mine["pending"] += n
        elif r.status == "rejected":
            mine["sent_back"] += n
    return mine


@app.get("/api/v1/stewardship/dashboard/exceptions-summary")
def exceptions_dashboard_summary(user: dict = Depends(auth_mod.require_steward_or_owner)):
    status_df = run_query("""
        SELECT COALESCE(o.remediation_status, eq.remediation_status) AS status, COUNT(*) AS n
        FROM main_silver.exceptions_queue eq
        LEFT JOIN stewardship.exception_status_overrides o
          ON o.exception_id = eq.exception_id
        GROUP BY 1
        ORDER BY 1
    """)
    reason_df = run_query("""
        SELECT reason, COUNT(*) AS n
        FROM (SELECT UNNEST(reject_reasons) AS reason FROM main_silver.exceptions_queue)
        GROUP BY 1
        ORDER BY n DESC
    """)
    created_df = run_query("""
        SELECT CAST(queued_ts AS DATE) AS d, COUNT(*) AS n
        FROM main_silver.exceptions_queue
        GROUP BY 1
        ORDER BY 1
    """)
    resolved_df = run_query("""
        SELECT CAST(updated_ts AS DATE) AS d, remediation_status AS status, COUNT(*) AS n
        FROM stewardship.exception_status_overrides
        WHERE remediation_status IN ('resolved', 'rejected')
        GROUP BY 1, 2
        ORDER BY 1
    """)

    trend = {}
    for r in created_df.itertuples():
        d = str(r.d)
        trend.setdefault(d, {"date": d, "created": 0, "resolved": 0, "rejected": 0})
        trend[d]["created"] += int(r.n)
    for r in resolved_df.itertuples():
        d = str(r.d)
        trend.setdefault(d, {"date": d, "created": 0, "resolved": 0, "rejected": 0})
        trend[d][r.status] += int(r.n)

    total = int(status_df["n"].sum()) if not status_df.empty else 0
    return {
        "total": total,
        "by_status": [{"status": r.status, "count": int(r.n)} for r in status_df.itertuples()],
        "by_reason": [{"reason": r.reason, "count": int(r.n)} for r in reason_df.itertuples()],
        "trend": sorted(trend.values(), key=lambda x: x["date"]),
        "your_activity": _my_workflow_activity("stewardship_remediation", user["user_id"]),
    }


@app.get("/api/v1/stewardship/dashboard/match-review-summary")
def match_review_dashboard_summary(user: dict = Depends(auth_mod.require_steward_or_owner)):
    status_df = run_query("""
        SELECT COALESCE(o.status, q.review_status) AS status, COUNT(*) AS n
        FROM main_gold.gold_match_review_queue q
        LEFT JOIN stewardship.match_review_overrides o
          ON o.pair_id = q.pair_id
        GROUP BY 1
        ORDER BY 1
    """)
    total = int(status_df["n"].sum()) if not status_df.empty else 0

    # Bucket similarity scores against the *live* Tier 2 thresholds -- shared
    # with the similarity_bucket filter on GET /stewardship/match-review so a
    # dashboard bar's count and its drill-through list can never disagree.
    low, high, third = _match_similarity_bucket_bounds()

    scores_df = run_query("SELECT similarity_score FROM main_gold.gold_match_review_queue")
    buckets = [
        {"bucket_index": 0, "range": f"{low:.2f}–{low + third:.2f}", "count": 0},
        {"bucket_index": 1, "range": f"{low + third:.2f}–{low + 2 * third:.2f}", "count": 0},
        {"bucket_index": 2, "range": f"{low + 2 * third:.2f}–{high:.2f}", "count": 0},
    ]
    for s in scores_df["similarity_score"]:
        s = float(s)
        if s < low + third:
            buckets[0]["count"] += 1
        elif s < low + 2 * third:
            buckets[1]["count"] += 1
        else:
            buckets[2]["count"] += 1

    return {
        "total": total,
        "by_status": [{"status": r.status, "count": int(r.n)} for r in status_df.itertuples()],
        "similarity_buckets": buckets,
        "thresholds": {"review_lower": low, "auto_merge": high},
        "your_activity": _my_workflow_activity("match_review_confirmation", user["user_id"]),
    }


@app.get("/health")
def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Administration -> User Administration (admin role required)
# ---------------------------------------------------------------------------

@app.get("/api/v1/admin/users")
def list_users(limit: int = 10, offset: int = 0, admin: dict = Depends(auth_mod.require_admin)):
    df = run_query("""
        SELECT user_id, username, full_name, role, gold_access, is_active, created_ts, last_login_ts
        FROM auth.users ORDER BY created_ts LIMIT ? OFFSET ?
    """, [limit, offset])
    total = run_query("SELECT COUNT(*) AS n FROM auth.users").iloc[0]["n"]
    return {"total": int(total), "limit": limit, "offset": offset, "users": to_records(df)}


VALID_ROLES = ("admin", "dataSteward", "dataOwner", "businessUser")


class CreateUserRequest(BaseModel):
    username: str
    full_name: str
    role: str          # 'admin' | 'dataSteward' | 'dataOwner' | 'businessUser'
    gold_access: str   # 'read_write' | 'read' | 'none'


@app.post("/api/v1/admin/users")
def create_user_endpoint(body: CreateUserRequest, admin: dict = Depends(auth_mod.require_admin)):
    """Submits a new-user request into the 'user_admin_change' maker-checker
    workflow (1 Admin, different from whoever requested it) instead of
    creating the account immediately. The one-time password is only
    generated -- and only shown -- once that Admin approves; see
    _execute_user_create below."""
    if body.role not in VALID_ROLES:
        raise HTTPException(400, f"role must be one of: {', '.join(VALID_ROLES)}")
    if body.gold_access not in ("read_write", "read", "none"):
        raise HTTPException(400, "gold_access must be one of: read_write, read, none")

    existing = run_query("SELECT user_id FROM auth.users WHERE username = ?", [body.username])
    if not existing.empty:
        raise HTTPException(409, f"Username '{body.username}' already exists")

    try:
        instance = wf_mod.start_workflow(
            "user_admin_change", entity_type="user", entity_id=f"new:{body.username}", maker=admin,
            action_type="create",
            payload={"username": body.username, "full_name": body.full_name,
                     "role": body.role, "gold_access": body.gold_access},
        )
    except wf_mod.WorkflowError as e:
        raise _wf_error(e)

    return {
        "status": "pending_approval",
        "workflow_instance_id": instance["instance_id"],
        "steps": instance["steps"],
        "note": "This user will be created once a different Admin approves the request. "
                "The one-time password is generated at approval time and shown only to that approver.",
    }


def _execute_user_create(instance: dict, actor: dict) -> dict:
    """on_approved callback for 'user_admin_change' (action_type='create')."""
    p = instance["payload"]
    try:
        result = auth_mod.create_user(p["username"], p["full_name"], p["role"], p["gold_access"])
    except ValueError as e:
        return {"error": str(e)}
    return {"action": "created", **result}  # includes the one-time temp_password


class UpdateUserRequest(BaseModel):
    full_name: Optional[str] = None
    role: Optional[str] = None
    gold_access: Optional[str] = None
    is_active: Optional[bool] = None


SENSITIVE_USER_FIELDS = {"role", "gold_access", "is_active"}


@app.put("/api/v1/admin/users/{user_id}")
def update_user(user_id: str, body: UpdateUserRequest, admin: dict = Depends(auth_mod.require_admin)):
    """Non-sensitive changes (full_name only) apply immediately. Any change
    touching role, gold_access, or is_active -- the fields that actually grant
    or remove access -- is instead submitted into the 'user_admin_change'
    maker-checker workflow (1 Admin, different from whoever requested it)."""
    existing = run_query("SELECT * FROM auth.users WHERE user_id = ?", [user_id])
    if existing.empty:
        raise HTTPException(404, "User not found")

    if body.role is not None and body.role not in VALID_ROLES:
        raise HTTPException(400, f"role must be one of: {', '.join(VALID_ROLES)}")

    updates = {k: v for k, v in body.dict().items() if v is not None}
    if not updates:
        return to_records(existing)[0]

    if SENSITIVE_USER_FIELDS & updates.keys():
        try:
            instance = wf_mod.start_workflow(
                "user_admin_change", entity_type="user", entity_id=user_id, maker=admin,
                action_type="update", payload={"updates": updates},
            )
        except wf_mod.WorkflowError as e:
            raise _wf_error(e)
        return {
            "user_id": user_id,
            "status": "pending_approval",
            "workflow_instance_id": instance["instance_id"],
            "steps": instance["steps"],
        }

    set_clause = ", ".join(f"{col} = ?" for col in updates)
    run_write(f"UPDATE auth.users SET {set_clause} WHERE user_id = ?", list(updates.values()) + [user_id])
    updated = run_query("""
        SELECT user_id, username, full_name, role, gold_access, is_active, created_ts, last_login_ts
        FROM auth.users WHERE user_id = ?
    """, [user_id])
    return to_records(updated)[0]


def _execute_user_update(instance: dict, actor: dict) -> dict:
    """on_approved callback for 'user_admin_change' (action_type='update')."""
    user_id = instance["entity_id"]
    updates = instance["payload"]["updates"]
    existing = run_query("SELECT user_id FROM auth.users WHERE user_id = ?", [user_id])
    if existing.empty:
        return {"error": f"user_id {user_id} no longer exists"}
    set_clause = ", ".join(f"{col} = ?" for col in updates)
    run_write(f"UPDATE auth.users SET {set_clause} WHERE user_id = ?", list(updates.values()) + [user_id])
    updated = run_query("""
        SELECT user_id, username, full_name, role, gold_access, is_active, created_ts, last_login_ts
        FROM auth.users WHERE user_id = ?
    """, [user_id])
    return {"action": "updated", "user": to_records(updated)[0]}


def _dispatch_user_admin_change(instance: dict, actor: dict) -> dict:
    """The 'user_admin_change' workflow_type covers both create and update
    actions sharing one approval chain; picks the right executor."""
    if instance["action_type"] == "create":
        return _execute_user_create(instance, actor)
    elif instance["action_type"] == "update":
        return _execute_user_update(instance, actor)
    raise ValueError(f"Unknown action_type '{instance['action_type']}' for user_admin_change")


@app.post("/api/v1/admin/users/{user_id}/reset_password")
def reset_password(user_id: str, admin: dict = Depends(auth_mod.require_admin)):
    df = run_query("SELECT username FROM auth.users WHERE user_id = ?", [user_id])
    if df.empty:
        raise HTTPException(404, "User not found")
    import secrets as _secrets
    new_password = _secrets.token_urlsafe(9)
    run_write("UPDATE auth.users SET password_hash = ? WHERE user_id = ?",
              [auth_mod.hash_password(new_password), user_id])
    return {"user_id": user_id, "username": df.iloc[0]["username"], "temp_password": new_password}


# ---------------------------------------------------------------------------
# Data Governance > Reference Data Maintenance: country codes and state
# codes. Read (GET) is open to any authenticated user; write (POST) is gated
# to dataSteward/dataOwner (auth_mod.require_steward_or_owner) and every
# change -- create, update, or deactivate (is_active=false; there is no hard
# delete, same convention as auth.users/column_rules/matching_rules) -- is
# submitted into the 'reference_data_change' maker-checker workflow (1 Data
# Owner, different from whoever requested it) instead of applied directly.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Phase 1 (flexibility): governance.entity_type_registry is the single
# metadata-driven catalog for every Reference Data Maintenance and Rules
# Configuration entity type (schema/seed in scripts/ensure_governed_schemas.py
# and api/db.py's _ensure_entity_type_registry). It replaces the
# REFERENCE_ENTITY_CONFIG / RULES_ENTITY_CONFIG dicts that used to live here,
# and GET /api/v1/governed-entities (below) serves it to both frontends so
# they can render tabs, table columns, and create/edit forms without any
# per-entity-type JS. Adding a new reference entity or rule type is now:
# insert one entity_type_registry row (+ create its physical table once,
# same as any typed schema change) -- no edits to this file or either
# frontend. See scripts/ensure_governed_schemas.py's DEFAULT_ENTITY_TYPE_REGISTRY
# docstring for the field_schema JSON shape.
#
# This only makes a new entity/rule *storable and editable*, not executable --
# that's Phase 2 (RULE_TYPE_HANDLERS-style dispatch in api/validation.py, the
# dbt models, and scripts/generate_matches.py).
# ---------------------------------------------------------------------------

def _load_entity_type_row(entity_type: str) -> Optional[dict]:
    """A single active entity_type_registry row, with field_schema parsed
    from JSON, or None if entity_type is unknown/inactive."""
    df = run_query("SELECT * FROM governance.entity_type_registry WHERE entity_type = ? AND active", [entity_type])
    if df.empty:
        return None
    row = to_records(df)[0]
    row["field_schema"] = json.loads(row["field_schema"]) if row.get("field_schema") else []
    return row


def _load_entity_catalog(category: Optional[str] = None) -> list:
    """All active entity_type_registry rows (optionally filtered to one
    category), field_schema parsed, ordered the way the frontend should
    display tabs -- category then sort_order."""
    if category:
        df = run_query(
            "SELECT * FROM governance.entity_type_registry WHERE active AND category = ? ORDER BY sort_order",
            [category],
        )
    else:
        df = run_query("SELECT * FROM governance.entity_type_registry WHERE active ORDER BY category, sort_order")
    records = to_records(df)
    for r in records:
        r["field_schema"] = json.loads(r["field_schema"]) if r.get("field_schema") else []
    return records


def _reference_data_cfg(entity_type: str) -> Optional[dict]:
    """Adapter from an entity_type_registry row to the {table, pk, name_field}
    shape the reference-data endpoints below use."""
    row = _load_entity_type_row(entity_type)
    if row is None or row["category"] != "reference_data":
        return None
    return {"table": row["table_name"], "pk": row["pk_column"], "name_field": row["name_field"]}


def _rules_config_cfg(entity_type: str) -> Optional[dict]:
    """Adapter from an entity_type_registry row to the
    {table, pk, id_prefix, columns, order_column} shape the rules-config
    endpoints below use. columns/order_column are derived from field_schema
    and pk_column rather than hand-maintained, so a new field on an existing
    rule type (add a row to DEFAULT_ENTITY_TYPE_REGISTRY's field_schema) is
    automatically writable and orderable with no further code change."""
    row = _load_entity_type_row(entity_type)
    if row is None or row["category"] != "rules_config":
        return None
    return {
        "table": row["table_name"],
        "pk": row["pk_column"],
        "id_prefix": row["id_prefix"],
        "columns": [f["key"] for f in row["field_schema"]],
        "order_column": row["order_column"] or row["pk_column"],
    }


def _validate_rules_config_values(entity_type: str, fields: dict):
    """Phase 0 (flexibility hardening), now registry-driven: rejects (400)
    any select-type field's value that isn't one of its declared options --
    instead of letting a rule_type/match_method/etc. with no real handler get
    submitted, approved by a Data Owner quorum, and then silently do nothing
    forever. A field's options list is also exactly what the frontend renders
    as its dropdown, so there's one source of truth for "what's a valid value
    for this field" instead of a client-side dropdown and a server-side
    allow-list that can drift apart."""
    row = _load_entity_type_row(entity_type)
    if row is None:
        return
    for f in row["field_schema"]:
        if f.get("type") != "select" or not f.get("options"):
            continue
        key = f["key"]
        if key not in fields or fields[key] is None:
            continue
        if str(fields[key]) not in f["options"]:
            raise HTTPException(
                400,
                f"Unknown {f['label']} '{fields[key]}' for {entity_type} -- no handler is "
                f"implemented for it yet, so it would be accepted but silently do nothing. "
                f"Known values: {', '.join(f['options'])}.",
            )


@app.get("/api/v1/governed-entities")
def list_governed_entities(category: Optional[str] = None, user: dict = Depends(auth_mod.get_current_user)):
    """Catalog of every Reference Data Maintenance ('reference_data') and
    Rules Configuration ('rules_config') entity type. Both portal_app's admin
    screens and both apps' Approvals diff-forms render entirely from this
    response -- see the module docstring above."""
    return {"items": _load_entity_catalog(category)}


class ReferenceDataRequest(BaseModel):
    entity_type: str          # 'ref_country_code' | 'ref_state_code'
    code: str                 # country_code or state_code value
    name: Optional[str] = None
    is_active: Optional[bool] = None


@app.get("/api/v1/reference-data/{entity_type}")
def list_reference_data(entity_type: str, limit: int = 10, offset: int = 0, user: dict = Depends(auth_mod.get_current_user)):
    cfg = _reference_data_cfg(entity_type)
    if cfg is None:
        raise HTTPException(404, f"Unknown reference data entity_type '{entity_type}'")
    df = run_query(f"SELECT * FROM {cfg['table']} ORDER BY {cfg['pk']} LIMIT ? OFFSET ?", [limit, offset])
    total = run_query(f"SELECT COUNT(*) AS n FROM {cfg['table']}").iloc[0]["n"]
    return {"entity_type": entity_type, "total": int(total), "limit": limit, "offset": offset, "items": to_records(df)}


@app.post("/api/v1/reference-data")
def submit_reference_data_change(body: ReferenceDataRequest, user: dict = Depends(auth_mod.require_steward_or_owner)):
    """Submits a create/update/deactivate of a country or state code row into
    the 'reference_data_change' maker-checker workflow instead of writing it
    directly -- see _execute_reference_data_change below."""
    cfg = _reference_data_cfg(body.entity_type)
    if cfg is None:
        raise HTTPException(404, f"Unknown reference data entity_type '{body.entity_type}'")
    if not body.code or not body.code.strip():
        raise HTTPException(400, "code is required")
    code = body.code.strip().upper()

    existing = run_query(f"SELECT * FROM {cfg['table']} WHERE {cfg['pk']} = ?", [code])
    action_type = "update" if not existing.empty else "create"
    if action_type == "create" and not body.name:
        raise HTTPException(400, f"{cfg['name_field']} is required when creating a new code")

    payload = {"entity_type": body.entity_type, "code": code, "name": body.name, "is_active": body.is_active}
    try:
        instance = wf_mod.start_workflow(
            "reference_data_change", entity_type=body.entity_type, entity_id=code, maker=user,
            action_type=action_type, payload=payload,
        )
    except wf_mod.WorkflowError as e:
        raise _wf_error(e)

    return {
        "entity_type": body.entity_type, "code": code,
        "status": "pending_approval",
        "workflow_instance_id": instance["instance_id"],
        "steps": instance["steps"],
    }


def _execute_reference_data_change(instance: dict, actor: dict) -> dict:
    """on_approved callback for 'reference_data_change'."""
    p = instance["payload"]
    cfg = _reference_data_cfg(p["entity_type"])
    if cfg is None:
        return {"error": f"Unknown entity_type '{p['entity_type']}'"}
    code = p["code"]
    name_col = cfg["name_field"]

    if instance["action_type"] == "create":
        existing = run_query(f"SELECT {cfg['pk']} FROM {cfg['table']} WHERE {cfg['pk']} = ?", [code])
        if not existing.empty:
            return {"error": f"{code} already exists -- nothing to create"}
        run_write(
            f"INSERT INTO {cfg['table']} ({cfg['pk']}, {name_col}, is_active, updated_by) VALUES (?, ?, ?, ?)",
            [code, p.get("name"), p.get("is_active") if p.get("is_active") is not None else True, actor.get("user_id")],
        )
        action = "created"
    else:
        existing = run_query(f"SELECT {cfg['pk']} FROM {cfg['table']} WHERE {cfg['pk']} = ?", [code])
        if existing.empty:
            return {"error": f"{code} no longer exists -- nothing to update"}
        set_parts, params = [], []
        if p.get("name") is not None:
            set_parts.append(f"{name_col} = ?")
            params.append(p["name"])
        if p.get("is_active") is not None:
            set_parts.append("is_active = ?")
            params.append(p["is_active"])
        if not set_parts:
            return {"action": "no_changes", "code": code}
        set_parts.append("updated_ts = current_timestamp")
        set_parts.append("updated_by = ?")
        params.append(actor.get("user_id"))
        params.append(code)
        run_write(f"UPDATE {cfg['table']} SET {', '.join(set_parts)} WHERE {cfg['pk']} = ?", params)
        action = "updated"

    updated_row = run_query(f"SELECT * FROM {cfg['table']} WHERE {cfg['pk']} = ?", [code])
    return {"action": action, "entity_type": p["entity_type"], "row": to_records(updated_row)[0]}


# ---------------------------------------------------------------------------
# Data Governance > Rules Configuration: column rules, matching rules,
# matching thresholds/tiers, and survivorship rules. Read (GET) is open to
# any authenticated user; write (POST) is gated to dataSteward/dataOwner
# (auth_mod.require_steward_or_owner) and every change -- create, update, or
# deactivate -- is submitted into the 'rules_config_change' maker-checker
# workflow (quorum of 2 Data Owners -- higher-risk than reference data since
# it can change what the batch pipeline rejects/matches/survives) instead of
# applied directly. One workflow_type covers all four sub-tables (they share
# the identical approval shape); `entity_type` distinguishes which table a
# given instance targets.
# ---------------------------------------------------------------------------

class RulesConfigRequest(BaseModel):
    entity_type: str                  # 'column_rule' | 'matching_rule' | 'matching_threshold' | 'survivorship_rule'
    entity_id: Optional[str] = None   # rule_id/tier_id of the row being updated; omit/None to create a new row
    fields: dict = {}                 # column -> new value (create: all desired columns; update: only changed ones)


@app.get("/api/v1/rules-config/{entity_type}")
def list_rules_config(entity_type: str, limit: int = 10, offset: int = 0, user: dict = Depends(auth_mod.get_current_user)):
    cfg = _rules_config_cfg(entity_type)
    if cfg is None:
        raise HTTPException(404, f"Unknown rules config entity_type '{entity_type}'")
    order_col = cfg["order_column"]
    df = run_query(f"SELECT * FROM {cfg['table']} ORDER BY {order_col} LIMIT ? OFFSET ?", [limit, offset])
    total = run_query(f"SELECT COUNT(*) AS n FROM {cfg['table']}").iloc[0]["n"]
    return {"entity_type": entity_type, "total": int(total), "limit": limit, "offset": offset, "items": to_records(df)}


@app.post("/api/v1/rules-config")
def submit_rules_config_change(body: RulesConfigRequest, user: dict = Depends(auth_mod.require_steward_or_owner)):
    """Submits a create/update/deactivate of a column rule, matching rule,
    matching threshold/tier, or survivorship rule row into the
    'rules_config_change' maker-checker workflow instead of writing it
    directly -- see _execute_rules_config_change below."""
    cfg = _rules_config_cfg(body.entity_type)
    if cfg is None:
        raise HTTPException(404, f"Unknown rules config entity_type '{body.entity_type}'")

    unknown_fields = set(body.fields.keys()) - set(cfg["columns"])
    if unknown_fields:
        raise HTTPException(400, f"Unknown field(s) for {body.entity_type}: {', '.join(sorted(unknown_fields))}")
    _validate_rules_config_values(body.entity_type, body.fields)

    if body.entity_id:
        existing = run_query(f"SELECT {cfg['pk']} FROM {cfg['table']} WHERE {cfg['pk']} = ?", [body.entity_id])
        if existing.empty:
            raise HTTPException(404, f"{body.entity_type} '{body.entity_id}' not found")
        action_type = "update"
        workflow_entity_id = body.entity_id
    else:
        if not body.fields:
            raise HTTPException(400, "fields must not be empty when creating a new row")
        action_type = "create"
        workflow_entity_id = f"new:{body.entity_type}:{uuid.uuid4().hex[:8]}"

    payload = {"entity_type": body.entity_type, "entity_id": body.entity_id, "fields": body.fields}
    try:
        instance = wf_mod.start_workflow(
            "rules_config_change", entity_type=body.entity_type, entity_id=workflow_entity_id, maker=user,
            action_type=action_type, payload=payload,
        )
    except wf_mod.WorkflowError as e:
        raise _wf_error(e)

    return {
        "entity_type": body.entity_type, "entity_id": body.entity_id,
        "status": "pending_approval",
        "workflow_instance_id": instance["instance_id"],
        "steps": instance["steps"],
    }


def _execute_rules_config_change(instance: dict, actor: dict) -> dict:
    """on_approved callback for 'rules_config_change'."""
    p = instance["payload"]
    cfg = _rules_config_cfg(p["entity_type"])
    if cfg is None:
        return {"error": f"Unknown entity_type '{p['entity_type']}'"}
    fields = p.get("fields") or {}

    if instance["action_type"] == "create":
        new_id = f"{cfg['id_prefix']}{uuid.uuid4().hex[:8].upper()}"
        cols = [cfg["pk"]] + list(fields.keys())
        vals = [new_id] + list(fields.values())
        placeholders = ", ".join(["?"] * len(vals))
        run_write(f"INSERT INTO {cfg['table']} ({', '.join(cols)}) VALUES ({placeholders})", vals)
        row = to_records(run_query(f"SELECT * FROM {cfg['table']} WHERE {cfg['pk']} = ?", [new_id]))[0]
        return {"action": "created", "entity_type": p["entity_type"], "row": row}

    entity_id = p["entity_id"]
    existing = run_query(f"SELECT {cfg['pk']} FROM {cfg['table']} WHERE {cfg['pk']} = ?", [entity_id])
    if existing.empty:
        return {"error": f"{entity_id} no longer exists -- nothing to update"}
    if not fields:
        return {"action": "no_changes", "entity_id": entity_id}
    set_parts = [f"{col} = ?" for col in fields]
    params = list(fields.values())
    set_parts.append("updated_ts = current_timestamp")
    set_parts.append("updated_by = ?")
    params.append(actor.get("user_id"))
    params.append(entity_id)
    run_write(f"UPDATE {cfg['table']} SET {', '.join(set_parts)} WHERE {cfg['pk']} = ?", params)
    row = to_records(run_query(f"SELECT * FROM {cfg['table']} WHERE {cfg['pk']} = ?", [entity_id]))[0]
    return {"action": "updated", "entity_type": p["entity_type"], "row": row}


# ---------------------------------------------------------------------------
# workflow_type -> (on_approved, on_rejected) executor registry, used by
# decide_workflow_instance() above. Defined last because it references
# functions declared throughout this file; only looked up at call time.
# ---------------------------------------------------------------------------
_WORKFLOW_EXECUTORS = {
    "stewardship_remediation": (_dispatch_stewardship_remediation, _rollback_stewardship_submission),
    "gold_record_edit": (_execute_gold_edit, None),
    "match_review_confirmation": (_execute_match_review, _rollback_match_review_submission),
    "user_admin_change": (_dispatch_user_admin_change, None),
    "reference_data_change": (_execute_reference_data_change, None),
    "rules_config_change": (_execute_rules_config_change, None),
}


app.mount("/app", StaticFiles(directory=str(BASE_DIR / "stewardship_app" / "frontend"), html=True), name="stewardship_app")
app.mount("/portal", StaticFiles(directory=str(BASE_DIR / "portal_app" / "frontend"), html=True), name="portal_app")
