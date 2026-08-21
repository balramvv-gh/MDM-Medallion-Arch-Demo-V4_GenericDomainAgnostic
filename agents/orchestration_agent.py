"""
Orchestration Agent -- the 7th agent. Runs the batch pipeline
(Ingestion -> DQ -> Matching -> Survivorship -> Steward) in that order, on
a METADATA-DRIVEN schedule read from governance.orchestration_schedule
(see scripts/ensure_governed_schemas.py's ensure_orchestration_tables()),
rather than a schedule hardcoded in code or in an OS-level cron/Task
Scheduler entry.

Why metadata-driven, concretely: an OS scheduler (cron / Windows Task
Scheduler) is configured to invoke this agent FREQUENTLY (e.g. every 15
minutes, `--poll-interval` below) -- not at the actual desired pipeline
time. On each invocation, `check_and_run_due()` reads every active row in
governance.orchestration_schedule, evaluates its cron_expression with
croniter against that schedule_id's last run in
governance.orchestration_run_log, and only actually executes the pipeline
if a scheduled fire time has passed since the last run. Changing when the
pipeline runs is then a DB edit (eventually exposable through Rules
Configuration, like every other bus_rules/governance table), never an
OS-level config change or a code deploy.

Stage sequence and the cancel_stale_match_reviews/audit_pipeline_diff
placement: see agents/matching_agent.py's and agents/survivorship_agent.py's
docstrings for why those two scripts are folded into Matching/Survivorship
respectively rather than being their own stages. Steward Agent's stage
here is the read-only queue health check (agents/steward_agent.py) -- it
does not mutate anything, matching how this project's real human-in-the-
loop mutations stay real-time/API-triggered, not batch-scheduled.

Usage:
    python -m agents.orchestration_agent --run-now                 # ad hoc, uses SCHED001's stage_sequence
    python -m agents.orchestration_agent --run-now --schedule-id SCHED001
    python -m agents.orchestration_agent --serve --poll-interval 900   # polling loop, 15 min default
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone

from agents.base import AgentResult, BaseAgent, get_connection, new_run_id
from agents.dq_agent import DQAgent
from agents.ingestion_agent import IngestionAgent
from agents.matching_agent import MatchingAgent
from agents.steward_agent import StewardAgent
from agents.survivorship_agent import SurvivorshipAgent

STAGE_AGENTS = {
    "ingestion": IngestionAgent,
    "dq": DQAgent,
    "matching": MatchingAgent,
    "survivorship": SurvivorshipAgent,
    "steward": StewardAgent,
}


class OrchestrationAgent(BaseAgent):
    name = "orchestration"

    def _execute(self, payload: dict) -> tuple[dict, str]:
        """`run()` (inherited from BaseAgent) triggers one ad hoc pipeline
        run using payload {"schedule_id": ..., "triggered_by": ...}. The
        scheduled-polling path (check_and_run_due/serve) calls
        run_pipeline() directly instead, since it needs to evaluate
        multiple schedule rows without wrapping each in its own top-level
        AgentResult."""
        schedule_id = payload.get("schedule_id")
        triggered_by = payload.get("triggered_by", "manual")
        schedule = self._load_schedule(schedule_id) if schedule_id else self._load_schedule_by_name(
            payload.get("pipeline_name", "full_batch_pipeline")
        )
        if schedule is None:
            raise RuntimeError(f"No active orchestration_schedule row found (schedule_id={schedule_id!r})")
        run_log = self.run_pipeline(schedule, triggered_by=triggered_by)
        row_counts = {"run_id": run_log["run_id"], "stage_count": len(run_log["stage_results"])}
        detail = f"pipeline '{schedule['pipeline_name']}' finished status={run_log['status']}"
        return row_counts, detail

    # -- core execution ------------------------------------------------------

    def run_pipeline(self, schedule: dict, triggered_by: str = "manual") -> dict:
        run_id = new_run_id()
        started_at = datetime.now(timezone.utc)
        stage_results: list[dict] = []
        overall_status = "success"

        for stage_name in schedule["stage_sequence"]:
            agent_cls = STAGE_AGENTS.get(stage_name)
            if agent_cls is None:
                stage_results.append({
                    "stage": stage_name, "status": "failed",
                    "error": f"Unknown stage '{stage_name}' -- no agent registered in STAGE_AGENTS",
                })
                overall_status = "partial" if schedule.get("on_failure") != "halt" else "failed"
                if schedule.get("on_failure") == "halt":
                    break
                continue

            payload = self._payload_for_stage(stage_name, schedule)
            result: AgentResult = agent_cls().run(payload)
            stage_results.append(result.to_dict())

            if result.status == "failed":
                on_failure = schedule.get("on_failure", "halt")
                overall_status = "failed" if on_failure == "halt" else "partial"
                if on_failure == "halt":
                    break

        ended_at = datetime.now(timezone.utc)
        run_log = {
            "run_id": run_id,
            "schedule_id": schedule.get("schedule_id"),
            "pipeline_name": schedule["pipeline_name"],
            "triggered_by": triggered_by,
            "started_at": started_at.isoformat(),
            "ended_at": ended_at.isoformat(),
            "status": overall_status,
            "stage_results": stage_results,
        }
        self._log_run(run_log)
        return run_log

    @staticmethod
    def _payload_for_stage(stage_name: str, schedule: dict) -> dict:
        run_mode = schedule.get("run_mode", "auto")
        if stage_name == "ingestion":
            return {"run_mode": run_mode}
        if stage_name == "dq":
            # Let DQAgent auto-detect off the batch Ingestion just landed,
            # unless the schedule explicitly forces full/incremental.
            if run_mode == "full":
                return {"full_refresh": True}
            if run_mode == "incremental":
                return {"full_refresh": False}
            return {"full_refresh": None}
        return {}

    # -- schedule polling ------------------------------------------------

    def check_and_run_due(self) -> list[dict]:
        """Evaluates every active governance.orchestration_schedule row;
        runs the ones whose cron_expression has a fire time between their
        last logged run and now. Returns the list of run_log dicts for
        whatever actually ran (empty if nothing was due)."""
        from croniter import croniter

        con = get_connection(read_only=True)
        try:
            schedules = con.execute("""
                SELECT schedule_id, pipeline_name, cron_expression, timezone, stage_sequence,
                       run_mode, on_failure, active
                FROM governance.orchestration_schedule WHERE active
            """).fetchall()
            columns = [d[0] for d in con.description]
        finally:
            con.close()

        run_logs = []
        for row in schedules:
            schedule = dict(zip(columns, row))
            schedule["stage_sequence"] = list(schedule["stage_sequence"])
            last_run_ended = self._last_run_ended_at(schedule["schedule_id"])
            now = datetime.now(timezone.utc)
            base = last_run_ended or (now.replace(year=now.year - 1))  # never run yet -> definitely due
            next_fire = croniter(schedule["cron_expression"], base).get_next(datetime)
            if next_fire.tzinfo is None:
                next_fire = next_fire.replace(tzinfo=timezone.utc)
            if next_fire <= now:
                print(f"[orchestration] schedule_id={schedule['schedule_id']} is due "
                      f"(next_fire={next_fire.isoformat()}) -- running now.")
                run_logs.append(self.run_pipeline(schedule, triggered_by="scheduler"))
        return run_logs

    def serve(self, poll_interval_seconds: int = 900) -> None:
        """Polling loop -- see module docstring for why the OS-level
        scheduler only needs to invoke this frequently, not at the actual
        pipeline time."""
        print(f"[orchestration] polling governance.orchestration_schedule every {poll_interval_seconds}s")
        while True:
            try:
                self.check_and_run_due()
            except Exception as exc:  # noqa: BLE001 -- keep the poll loop alive across a bad run
                print(f"[orchestration] check_and_run_due() raised: {exc}")
            time.sleep(poll_interval_seconds)

    # -- persistence ------------------------------------------------------

    @staticmethod
    def _load_schedule(schedule_id: str) -> dict | None:
        con = get_connection(read_only=True)
        try:
            row = con.execute("""
                SELECT schedule_id, pipeline_name, cron_expression, timezone, stage_sequence,
                       run_mode, on_failure, active
                FROM governance.orchestration_schedule WHERE schedule_id = ? AND active
            """, [schedule_id]).fetchone()
            if row is None:
                return None
            columns = [d[0] for d in con.description]
            schedule = dict(zip(columns, row))
            schedule["stage_sequence"] = list(schedule["stage_sequence"])
            return schedule
        finally:
            con.close()

    @staticmethod
    def _load_schedule_by_name(pipeline_name: str) -> dict | None:
        con = get_connection(read_only=True)
        try:
            row = con.execute("""
                SELECT schedule_id, pipeline_name, cron_expression, timezone, stage_sequence,
                       run_mode, on_failure, active
                FROM governance.orchestration_schedule
                WHERE pipeline_name = ? AND active
                ORDER BY schedule_id LIMIT 1
            """, [pipeline_name]).fetchone()
            if row is None:
                return None
            columns = [d[0] for d in con.description]
            schedule = dict(zip(columns, row))
            schedule["stage_sequence"] = list(schedule["stage_sequence"])
            return schedule
        finally:
            con.close()

    @staticmethod
    def _last_run_ended_at(schedule_id: str):
        con = get_connection(read_only=True)
        try:
            row = con.execute("""
                SELECT ended_at FROM governance.orchestration_run_log
                WHERE schedule_id = ? AND status IN ('success', 'partial', 'failed')
                ORDER BY ended_at DESC LIMIT 1
            """, [schedule_id]).fetchone()
        finally:
            con.close()
        if row is None or row[0] is None:
            return None
        ts = row[0]
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)

    @staticmethod
    def _log_run(run_log: dict) -> None:
        con = get_connection(read_only=False)
        try:
            con.execute("""
                INSERT INTO governance.orchestration_run_log
                    (run_id, schedule_id, pipeline_name, triggered_by, started_at, ended_at, status, stage_results)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                run_log["run_id"], run_log["schedule_id"], run_log["pipeline_name"],
                run_log["triggered_by"], run_log["started_at"], run_log["ended_at"],
                run_log["status"], json.dumps(run_log["stage_results"], default=str),
            ])
        finally:
            con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-now", action="store_true", help="Run one pipeline immediately (ad hoc).")
    parser.add_argument("--schedule-id", default="SCHED001", help="orchestration_schedule row to use.")
    parser.add_argument("--serve", action="store_true", help="Start the metadata-driven polling loop.")
    parser.add_argument("--poll-interval", type=int, default=900, help="Seconds between schedule checks.")
    args = parser.parse_args()

    agent = OrchestrationAgent()
    if args.serve:
        agent.serve(poll_interval_seconds=args.poll_interval)
    elif args.run_now:
        schedule = agent._load_schedule(args.schedule_id)
        if schedule is None:
            raise SystemExit(f"No active schedule found for schedule_id={args.schedule_id!r}")
        result = agent.run_pipeline(schedule, triggered_by="manual:cli")
        print(json.dumps(result, indent=2, default=str))
    else:
        parser.print_help()
