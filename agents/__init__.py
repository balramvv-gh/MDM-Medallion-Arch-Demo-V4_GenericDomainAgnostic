"""
Stateless pipeline agents for the MDM Medallion Demo.

Each agent wraps one stage of the existing dbt-core + DuckDB batch pipeline
(previously orchestrated procedurally by scripts/build_pipeline.py) as a
self-contained, independently invocable unit with a uniform contract:

    result: AgentResult = SomeAgent().run(payload: dict)

Agents are STATELESS by design (see base.py's module docstring for what that
means concretely here) so any number of them can be started, scaled, or
restarted independently -- state lives in DuckDB (or the underlying
warehouse in a real deployment), never in agent process memory. The MCP
server (see mcp_server/) is the only thing that sequences them; agents never
call each other directly.

Business logic is NOT reimplemented here -- each agent is a thin wrapper
that invokes the same tested scripts/*.py and dbt commands
scripts/build_pipeline.py already runs, so behavior stays identical to the
existing batch pipeline. See MCP_Agent_Orchestration_Design.docx for the
full architecture and rationale.
"""
