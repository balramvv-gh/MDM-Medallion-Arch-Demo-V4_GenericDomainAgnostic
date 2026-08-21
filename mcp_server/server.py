"""
The MCP server process: the central orchestrator and tool registry
referenced throughout MCP_Agent_Orchestration_Design.docx. Exposes every
ToolSpec in mcp_server/tool_registry.py as an MCP tool via the official
`mcp` Python SDK's FastMCP server, over stdio by default (works with any
MCP-compatible client -- Claude Desktop/Code, a custom orchestrator
process, etc. -- with zero code change; swap transport to 'sse' or
'streamable-http' for a networked deployment, see main() below).

This process itself holds no pipeline state -- every tool call opens its
own DuckDB connection via the agent it dispatches to (see agents/base.py's
statelessness docstring) and returns a plain dict. Any number of these
server processes could run at once against the same DuckDB file for read
paths; write paths (dbt runs, generate_matches.py, load_bronze.py) are
serialized by DuckDB's single-writer lock regardless of how many server
processes exist -- see MCP_Agent_Orchestration_Design.docx's "Concurrency"
section.

Run:
    python -m mcp_server.server                  # stdio transport (default)
    python -m mcp_server.server --transport sse --port 8765
"""
import argparse
from typing import Optional

from mcp.server.fastmcp import FastMCP

from mcp_server.tool_registry import REGISTRY

mcp = FastMCP(
    name="mdm-medallion-orchestrator",
    instructions=(
        "Tool registry for the MDM Medallion Demo's 7-agent pipeline "
        "(Ingestion, DQ, Matching, Survivorship, Steward, Monitoring, "
        "Orchestration). Call orchestration.run_pipeline to run the full "
        "batch sequence, or an individual agent's *.run tool to run one "
        "stage. monitoring.check_health / monitoring.get_metrics are "
        "read-only and safe to call anytime."
    ),
)


def _register_tools() -> None:
    """Registers every ToolSpec from the registry as an MCP tool. Uses a
    generic `payload: dict` parameter for every tool rather than one
    hand-written wrapper function per tool -- the registry is already the
    single source of truth for name/description/handler, so this loop is
    what keeps server.py from having to be edited every time a tool is
    added to tool_registry.py."""
    for tool_name, spec in REGISTRY.items():
        def _make_tool(spec=spec):
            def _tool(payload: Optional[dict] = None) -> dict:  # noqa: A002
                return spec.handler(payload or {})
            _tool.__name__ = spec.name.replace(".", "_")
            _tool.__doc__ = spec.description
            return _tool

        mcp.tool(name=tool_name, description=spec.description)(_make_tool())


_register_tools()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--transport", choices=["stdio", "sse", "streamable-http"], default="stdio",
        help="MCP transport. 'stdio' for a local subprocess client (default). "
             "'sse'/'streamable-http' for a networked server other agents/Databricks jobs can reach.",
    )
    args = parser.parse_args()
    mcp.run(transport=args.transport)


if __name__ == "__main__":
    main()
