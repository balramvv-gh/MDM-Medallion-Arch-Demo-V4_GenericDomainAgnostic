"""
Steward Agent -- Human-in-the-loop workflows.

Today, all of this project's actual human-in-the-loop mutation logic
(exception remediation, Match Review confirm/reject, the maker-checker
approval chains) is REAL-TIME and API-triggered from the Stewardship
Console/Portal -- api/main.py calls api/reprocessing.py and
api/workflow_engine.py directly the instant a steward acts, there is no
batch step that does this work today. See scripts/build_pipeline.py: it
has no "steward" stage at all.

So this agent has two distinct kinds of tools, deliberately kept separate:

1. Batch-time (`run()` / `queue_health_check`) -- a read-only snapshot of
   the human-in-the-loop queues (open exceptions, pending Match Review
   pairs, workflow instances awaiting a decision), taken as the 5th stage
   of the scheduled batch sequence per the requested Ingestion -> DQ ->
   Matching -> Survivorship -> Steward order. This makes Steward Agent
   genuinely stateless and safe to run on a schedule (no writes), while
   still giving the Monitoring Agent / Orchestration Agent a per-run
   signal of queue health. It does NOT run cancel_stale_match_reviews.py
   -- that runs inside Matching Agent instead, because it's a correctness
   precondition for matching, not a steward action; see
   agents/matching_agent.py's docstring for the full reasoning.

2. Real-time delegates (`reprocess_exception`, `reprocess_match_confirmation`)
   -- thin wrappers around api/reprocessing.py's existing
   `reprocess_corrected_record`/`reprocess_confirmed_match_review`
   functions, exposed as MCP tools so the FastAPI app COULD eventually call
   through the MCP tool registry instead of importing api/reprocessing.py
   directly (single front door for all pipeline mutation, agent or human-
   triggered). Not wired into api/main.py yet in this scaffolding pass --
   api/main.py's existing call sites are untouched and continue to work
   exactly as before. Flagged as follow-up integration work in
   MCP_Agent_Orchestration_Design.docx so it can be done carefully and
   tested end-to-end rather than bundled into this change.
"""
from agents.base import AgentError, BaseAgent, get_connection


class StewardAgent(BaseAgent):
    name = "steward"

    def _execute(self, payload: dict) -> tuple[dict, str]:
        row_counts = self._queue_health()
        detail = (
            f"{row_counts.get('exceptions_open', '?')} open exceptions, "
            f"{row_counts.get('match_review_pending', '?')} pending Match Review pairs, "
            f"{row_counts.get('workflows_pending', '?')} workflow instances awaiting decision"
        )
        return row_counts, detail

    @staticmethod
    def _queue_health() -> dict:
        con = get_connection(read_only=True)
        try:
            def table_exists(schema, table):
                return con.execute("""
                    SELECT count(*) FROM information_schema.tables
                    WHERE table_schema = ? AND table_name = ?
                """, [schema, table]).fetchone()[0] > 0

            counts = {}

            if table_exists("main_silver", "exceptions_queue"):
                sql = "SELECT count(*) FROM main_silver.exceptions_queue eq"
                if table_exists("stewardship", "exception_status_overrides"):
                    sql += """
                        LEFT JOIN stewardship.exception_status_overrides o
                          ON o.exception_id = eq.exception_id
                        WHERE COALESCE(o.remediation_status, eq.remediation_status) = 'open'
                    """
                else:
                    sql += " WHERE eq.remediation_status = 'open'"
                counts["exceptions_open"] = con.execute(sql).fetchone()[0]
            else:
                counts["exceptions_open"] = None

            if table_exists("main_gold", "gold_match_review_queue"):
                sql = """
                    SELECT count(*) FROM main_gold.gold_match_review_queue q
                """
                if table_exists("stewardship", "match_review_overrides"):
                    sql += """
                        LEFT JOIN stewardship.match_review_overrides o ON o.pair_id = q.pair_id
                        WHERE COALESCE(o.status, q.review_status) IN ('pending', 'in_review')
                    """
                else:
                    sql += " WHERE q.review_status IN ('pending', 'in_review')"
                counts["match_review_pending"] = con.execute(sql).fetchone()[0]
            else:
                counts["match_review_pending"] = None

            if table_exists("governance", "workflow_instances"):
                counts["workflows_pending"] = con.execute("""
                    SELECT count(*) FROM governance.workflow_instances WHERE status = 'pending'
                """).fetchone()[0]
            else:
                counts["workflows_pending"] = None

            return counts
        finally:
            con.close()

    # -- Real-time delegates (see class docstring, item 2) -------------------
    #
    # api/reprocessing.py imports its sibling modules bare (`from db import
    # ...`, `import audit as audit_mod`), not as `api.db`/`api.audit` -- the
    # existing convention for how the FastAPI app itself is launched (the
    # api/ directory is on sys.path, e.g. `cd api && uvicorn main:app`), not
    # something this scaffolding pass should change. `_with_api_on_path()`
    # replicates that by inserting api/ onto sys.path before importing, so
    # these delegates work from agents/'s project-root context too.

    @staticmethod
    def _with_api_on_path():
        import sys

        from agents.base import PROJECT_ROOT

        api_dir = str(PROJECT_ROOT / "api")
        if api_dir not in sys.path:
            sys.path.insert(0, api_dir)

    def reprocess_exception(self, record: dict, actor: dict = None) -> dict:
        """Delegates to api/reprocessing.py's reprocess_corrected_record --
        NOT invoked from anywhere in this scaffolding pass yet (api/main.py
        still calls that function directly). Exposed here so the MCP tool
        registry can route to it once that integration is done."""
        self._with_api_on_path()
        try:
            from reprocessing import reprocess_corrected_record
        except ImportError as exc:
            raise AgentError(f"api/reprocessing.py not importable from this context: {exc}") from exc
        return reprocess_corrected_record(record, actor=actor)

    def reprocess_match_confirmation(self, pair_id: str, actor: dict = None) -> dict:
        """Delegates to api/reprocessing.py's reprocess_confirmed_match_review
        -- same not-yet-wired-in status as reprocess_exception above."""
        self._with_api_on_path()
        try:
            from reprocessing import reprocess_confirmed_match_review
        except ImportError as exc:
            raise AgentError(f"api/reprocessing.py not importable from this context: {exc}") from exc
        return reprocess_confirmed_match_review(pair_id, actor=actor)


if __name__ == "__main__":
    import json

    result = StewardAgent().run({})
    print(json.dumps(result.to_dict(), indent=2, default=str))
