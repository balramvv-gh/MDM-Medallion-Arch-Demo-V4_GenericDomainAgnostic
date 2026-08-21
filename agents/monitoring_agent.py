"""
Monitoring Agent -- SLAs, DQ metrics, pipeline health. NEW agent (no
existing script wraps this; it's net-new functionality, unlike the other
five agents which wrap tested pipeline steps).

Always read-only -- this agent never writes to the pipeline's data tables,
only reads governance.orchestration_run_log (written by the Orchestration
Agent) and the same queue tables the DQ/Matching/Steward agents already
query, then grades them against governance.monitoring_thresholds
(warning/critical bands, metadata-driven -- see
scripts/ensure_governed_schemas.py's ensure_monitoring_tables()).

Two tools:
  - check_health()  -> overall status ('healthy' | 'degraded' | 'critical')
                        plus the specific metric(s) that drove it.
  - get_metrics()   -> the full metric dump (pipeline staleness, last run's
                        per-stage durations, exception/match-review queue
                        depths), no grading -- for a dashboard or the
                        Monitoring Agent's own audit trail.

Payload: {} for both -- no parameters today.
"""
import json
from datetime import datetime, timezone

from agents.base import BaseAgent, get_connection


class MonitoringAgent(BaseAgent):
    name = "monitoring"

    def _execute(self, payload: dict) -> tuple[dict, str]:
        metrics = self.get_metrics()
        health = self._grade(metrics)
        row_counts = {"status": health["status"], **metrics}
        detail = health["summary"]
        return row_counts, detail

    # -- MCP-facing tools ------------------------------------------------

    def check_health(self) -> dict:
        metrics = self.get_metrics()
        return self._grade(metrics)

    def get_metrics(self) -> dict:
        con = get_connection(read_only=True)
        try:
            return {
                "pipeline_staleness_hours": self._pipeline_staleness_hours(con),
                "last_run": self._last_run_summary(con),
                "exceptions_open_count": self._exceptions_open_count(con),
                "match_review_pending_count": self._match_review_pending_count(con),
                "workflows_pending_count": self._workflows_pending_count(con),
            }
        finally:
            con.close()

    # -- internals ---------------------------------------------------------

    @staticmethod
    def _table_exists(con, schema, table) -> bool:
        return con.execute("""
            SELECT count(*) FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
        """, [schema, table]).fetchone()[0] > 0

    def _pipeline_staleness_hours(self, con) -> float | None:
        if not self._table_exists(con, "governance", "orchestration_run_log"):
            return None
        row = con.execute("""
            SELECT ended_at FROM governance.orchestration_run_log
            WHERE status = 'success'
            ORDER BY ended_at DESC LIMIT 1
        """).fetchone()
        if row is None or row[0] is None:
            return None
        ended_at = row[0]
        if ended_at.tzinfo is None:
            ended_at = ended_at.replace(tzinfo=timezone.utc)
        return round((datetime.now(timezone.utc) - ended_at).total_seconds() / 3600, 2)

    def _last_run_summary(self, con) -> dict | None:
        if not self._table_exists(con, "governance", "orchestration_run_log"):
            return None
        row = con.execute("""
            SELECT run_id, pipeline_name, status, started_at, ended_at, stage_results
            FROM governance.orchestration_run_log
            ORDER BY started_at DESC LIMIT 1
        """).fetchone()
        if row is None:
            return None
        return {
            "run_id": row[0],
            "pipeline_name": row[1],
            "status": row[2],
            "started_at": str(row[3]),
            "ended_at": str(row[4]) if row[4] else None,
            "stages": json.loads(row[5]) if row[5] else [],
        }

    def _exceptions_open_count(self, con) -> int | None:
        if not self._table_exists(con, "main_silver", "exceptions_queue"):
            return None
        sql = "SELECT count(*) FROM main_silver.exceptions_queue eq"
        if self._table_exists(con, "stewardship", "exception_status_overrides"):
            sql += """
                LEFT JOIN stewardship.exception_status_overrides o ON o.exception_id = eq.exception_id
                WHERE COALESCE(o.remediation_status, eq.remediation_status) = 'open'
            """
        else:
            sql += " WHERE eq.remediation_status = 'open'"
        return con.execute(sql).fetchone()[0]

    def _match_review_pending_count(self, con) -> int | None:
        if not self._table_exists(con, "main_gold", "gold_match_review_queue"):
            return None
        sql = "SELECT count(*) FROM main_gold.gold_match_review_queue q"
        if self._table_exists(con, "stewardship", "match_review_overrides"):
            sql += """
                LEFT JOIN stewardship.match_review_overrides o ON o.pair_id = q.pair_id
                WHERE COALESCE(o.status, q.review_status) IN ('pending', 'in_review')
            """
        else:
            sql += " WHERE q.review_status IN ('pending', 'in_review')"
        return con.execute(sql).fetchone()[0]

    def _workflows_pending_count(self, con) -> int | None:
        if not self._table_exists(con, "governance", "workflow_instances"):
            return None
        return con.execute(
            "SELECT count(*) FROM governance.workflow_instances WHERE status = 'pending'"
        ).fetchone()[0]

    @staticmethod
    def _thresholds(con) -> dict:
        if not MonitoringAgent._table_exists(con, "governance", "monitoring_thresholds"):
            return {}
        rows = con.execute("""
            SELECT metric_name, warning_threshold, critical_threshold
            FROM governance.monitoring_thresholds WHERE active
        """).fetchall()
        return {r[0]: (r[1], r[2]) for r in rows}

    def _grade(self, metrics: dict) -> dict:
        con = get_connection(read_only=True)
        try:
            thresholds = self._thresholds(con)
        finally:
            con.close()

        breaches = []  # (metric, level, value, warning, critical)
        checks = {
            "pipeline_staleness_hours": metrics.get("pipeline_staleness_hours"),
            "exceptions_open_count": metrics.get("exceptions_open_count"),
            "match_review_pending_count": metrics.get("match_review_pending_count"),
        }
        for metric_name, value in checks.items():
            if value is None or metric_name not in thresholds:
                continue
            warning, critical = thresholds[metric_name]
            if critical is not None and value >= critical:
                breaches.append((metric_name, "critical", value, warning, critical))
            elif warning is not None and value >= warning:
                breaches.append((metric_name, "warning", value, warning, critical))

        last_run = metrics.get("last_run")
        if last_run and last_run.get("status") == "failed":
            breaches.append(("last_run_status", "critical", "failed", None, None))
        elif last_run and last_run.get("status") == "partial":
            breaches.append(("last_run_status", "warning", "partial", None, None))

        if any(b[1] == "critical" for b in breaches):
            status = "critical"
        elif breaches:
            status = "degraded"
        else:
            status = "healthy"

        if breaches:
            summary = "; ".join(f"{m}={v} ({lvl})" for m, lvl, v, _, _ in breaches)
        else:
            summary = "all monitored metrics within threshold"

        return {"status": status, "breaches": breaches, "metrics": metrics, "summary": summary}


if __name__ == "__main__":
    result = MonitoringAgent().run({})
    print(json.dumps(result.to_dict(), indent=2, default=str))
