"""
Central tool registry: the canonical catalog of every capability the 7
agents expose, independent of the MCP transport layer (server.py just
loops over this and registers each ToolSpec as an MCP tool via FastMCP's
`@mcp.tool()`). Keeping the catalog here, separate from server.py, means:

  - The catalog can be inspected/tested without starting an actual MCP
    server process (see the __main__ block below, and
    scripts/verify_mcp_scaffolding.py).
  - A future second transport (e.g. exposing the same tools over a plain
    REST endpoint for a Databricks job, or a CLI) can reuse this registry
    instead of re-declaring every tool.
  - It documents, in one place, exactly what "the MCP server as tool
    registry" means for this project -- the seven agents' full public
    surface.

Each ToolSpec's `handler` takes a single `payload: dict` and returns a
plain dict (an AgentResult.to_dict(), or a smaller dict for the
non-pipeline-stage tools like monitoring.get_metrics). This mirrors
BaseAgent's contract deliberately -- see agents/base.py.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from agents.dq_agent import DQAgent
from agents.ingestion_agent import IngestionAgent
from agents.matching_agent import MatchingAgent
from agents.monitoring_agent import MonitoringAgent
from agents.orchestration_agent import OrchestrationAgent
from agents.steward_agent import StewardAgent
from agents.survivorship_agent import SurvivorshipAgent


@dataclass
class ToolSpec:
    name: str
    description: str
    handler: Callable[[dict], Any]


def _agent_run_handler(agent_cls):
    def handler(payload: dict) -> dict:
        return agent_cls().run(payload or {}).to_dict()
    return handler


def _build_registry() -> dict[str, ToolSpec]:
    specs = [
        ToolSpec(
            "ingestion.run",
            "Ingestion Agent: Source -> Bronze ingestion. Payload: "
            "{run_mode: 'auto'|'full'|'incremental'} (default 'auto').",
            _agent_run_handler(IngestionAgent),
        ),
        ToolSpec(
            "dq.run",
            "DQ Agent: profiling, validation, standardization (silver dbt run). "
            "Payload: {full_refresh: true|false|null} (default null = auto-detect).",
            _agent_run_handler(DQAgent),
        ),
        ToolSpec(
            "matching.run",
            "Matching Agent: identity resolution (stale-review cancellation + "
            "generate_matches.py). Payload: {} (no parameters).",
            _agent_run_handler(MatchingAgent),
        ),
        ToolSpec(
            "survivorship.run",
            "Survivorship Agent: attribute-level survivorship (gold dbt run + "
            "audit diff). Payload: {} (no parameters).",
            _agent_run_handler(SurvivorshipAgent),
        ),
        ToolSpec(
            "steward.queue_health",
            "Steward Agent: read-only snapshot of open exceptions / pending "
            "Match Review pairs / pending workflow approvals. Payload: {}.",
            _agent_run_handler(StewardAgent),
        ),
        ToolSpec(
            "steward.reprocess_exception",
            "Steward Agent: real-time delegate to api/reprocessing.py's "
            "reprocess_corrected_record. Payload: {record: {...}, actor: {...}|null}. "
            "NOT yet called from api/main.py -- see agents/steward_agent.py docstring.",
            lambda payload: StewardAgent().reprocess_exception(
                payload["record"], payload.get("actor")
            ),
        ),
        ToolSpec(
            "steward.reprocess_match_confirmation",
            "Steward Agent: real-time delegate to api/reprocessing.py's "
            "reprocess_confirmed_match_review. Payload: {pair_id: str, actor: {...}|null}. "
            "NOT yet called from api/main.py -- see agents/steward_agent.py docstring.",
            lambda payload: StewardAgent().reprocess_match_confirmation(
                payload["pair_id"], payload.get("actor")
            ),
        ),
        ToolSpec(
            "monitoring.check_health",
            "Monitoring Agent: overall status (healthy/degraded/critical) graded "
            "against governance.monitoring_thresholds. Payload: {}.",
            lambda payload: MonitoringAgent().check_health(),
        ),
        ToolSpec(
            "monitoring.get_metrics",
            "Monitoring Agent: raw metric dump (pipeline staleness, last run's "
            "stage durations, queue depths), ungraded. Payload: {}.",
            lambda payload: MonitoringAgent().get_metrics(),
        ),
        ToolSpec(
            "orchestration.run_pipeline",
            "Orchestration Agent: ad hoc/manual pipeline trigger, runs the "
            "schedule's stage_sequence in order and logs to "
            "governance.orchestration_run_log. Payload: "
            "{schedule_id: str|null, pipeline_name: str (default 'full_batch_pipeline'), "
            "triggered_by: str (default 'manual')}.",
            _agent_run_handler(OrchestrationAgent),
        ),
        ToolSpec(
            "orchestration.check_and_run_due",
            "Orchestration Agent: evaluates every active "
            "governance.orchestration_schedule row and runs any that are due "
            "per their cron_expression. Payload: {}. (This is what the polling "
            "loop in `python -m agents.orchestration_agent --serve` calls "
            "repeatedly -- exposed here too so an external scheduler, e.g. a "
            "Databricks job or Windows Task Scheduler entry, can trigger a "
            "single due-check via MCP instead of running the polling loop.)",
            lambda payload: {"runs": OrchestrationAgent().check_and_run_due()},
        ),
    ]
    return {spec.name: spec for spec in specs}


REGISTRY: dict[str, ToolSpec] = _build_registry()


def call_tool(name: str, payload: dict | None = None) -> Any:
    """Invokes a registered tool by name outside the MCP transport --
    used by tests/verification and by anything running in-process that
    wants the registry's uniform dispatch without paying for a protocol
    round-trip."""
    if name not in REGISTRY:
        raise KeyError(f"Unknown tool '{name}'. Known tools: {sorted(REGISTRY)}")
    return REGISTRY[name].handler(payload or {})


if __name__ == "__main__":
    for tool_name, spec in sorted(REGISTRY.items()):
        print(f"{tool_name}\n    {spec.description}\n")
