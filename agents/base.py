"""
Shared contract for all pipeline agents: AgentResult, BaseAgent, and the
subprocess/DuckDB helpers every agent uses to invoke the existing pipeline
scripts and dbt commands.

Statelessness, concretely: a BaseAgent subclass holds no instance state
that outlives a single `run()` call -- no cached DuckDB connection, no
in-memory queue, no dependency on a previous invocation's result. Every
`run()`:
  1. opens its own short-lived DuckDB connection (or none, if it only
     shells out to dbt/a script that opens its own),
  2. does its work,
  3. closes the connection,
  4. returns a plain-data AgentResult.
This is what makes "agents can be scaled independently" true even though
they all point at one DuckDB file today: any number of agent processes can
be started/stopped/restarted without coordinating with each other, because
none of them remember anything between calls. (DuckDB itself only allows
one writer at a time -- see MCP_Agent_Orchestration_Design.docx's
"Concurrency" section for why the Orchestration Agent still runs batch
stages sequentially rather than in parallel, and what changes in a real
multi-node deployment on Databricks/a client-server warehouse.)

Windows portability: every path is computed via pathlib relative to this
file's location, matching the rest of the project's convention -- no
hardcoded absolute paths.
"""
from __future__ import annotations

import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
DB_PATH = str(PROJECT_ROOT / "mdm_demo.duckdb")


@dataclass
class AgentResult:
    """Uniform return shape for every agent's run(), and every MCP tool
    built on top of one -- so the Orchestration Agent (and Monitoring
    Agent, reading these back out of governance.orchestration_run_log) can
    treat all six agents identically without stage-specific parsing."""

    agent: str
    stage: str
    status: str  # 'success' | 'failed'
    started_at: str
    ended_at: str
    duration_seconds: float
    row_counts: dict[str, Any] = field(default_factory=dict)
    detail: str = ""
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "agent": self.agent,
            "stage": self.stage,
            "status": self.status,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "duration_seconds": self.duration_seconds,
            "row_counts": self.row_counts,
            "detail": self.detail,
            "error": self.error,
        }


class AgentError(Exception):
    """Raised by an agent's internal work when a stage fails -- caught by
    BaseAgent.run() and turned into a status='failed' AgentResult rather
    than propagating, so a single stage's failure never crashes whatever
    process (MCP server, orchestration loop, a script's __main__) invoked
    the agent. The Orchestration Agent is the one place that decides
    whether a failure halts the rest of the sequence (per
    governance.orchestration_schedule.on_failure)."""


class BaseAgent:
    """Subclass and implement `_execute(self, payload: dict) -> tuple[dict, str]`
    returning (row_counts, detail). Raise AgentError (or let a real
    exception surface -- it's caught the same way) for failures."""

    name: str = "base"

    def run(self, payload: Optional[dict] = None) -> AgentResult:
        payload = payload or {}
        started = time.time()
        started_at = _now_iso()
        try:
            row_counts, detail = self._execute(payload)
            ended = time.time()
            return AgentResult(
                agent=self.name,
                stage=self.name,
                status="success",
                started_at=started_at,
                ended_at=_now_iso(),
                duration_seconds=round(ended - started, 3),
                row_counts=row_counts,
                detail=detail,
            )
        except Exception as exc:  # noqa: BLE001 -- deliberately broad, see class docstring
            ended = time.time()
            return AgentResult(
                agent=self.name,
                stage=self.name,
                status="failed",
                started_at=started_at,
                ended_at=_now_iso(),
                duration_seconds=round(ended - started, 3),
                row_counts={},
                detail="",
                error=str(exc),
            )

    def _execute(self, payload: dict) -> tuple[dict, str]:
        raise NotImplementedError


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def new_run_id() -> str:
    return str(uuid.uuid4())


def run_subprocess(cmd: list[str], cwd: Optional[Path] = None, env: Optional[dict] = None) -> tuple[int, str, str]:
    """Runs a command exactly the way scripts/build_pipeline.py's `run()`
    helper does (same executable, same cwd/env conventions) but CAPTURES
    stdout/stderr instead of only streaming to the console -- an agent
    needs the output to extract row counts and surface errors back through
    its AgentResult, whereas build_pipeline.py just needed a pass/fail
    exit code."""
    print(f"\n$ {' '.join(cmd)}")
    result = subprocess.run(
        cmd, cwd=cwd, env=env, capture_output=True, text=True,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    return result.returncode, result.stdout, result.stderr


def run_python_script(script_name: str, args: Optional[list[str]] = None) -> tuple[int, str, str]:
    """Runs a scripts/*.py module the same way build_pipeline.py does
    (`sys.executable scripts/<name>.py ...`), preserving the existing
    tested logic instead of reimplementing it inside the agent."""
    cmd = [sys.executable, str(SCRIPTS_DIR / script_name)] + (args or [])
    return run_subprocess(cmd)


def run_dbt(args: list[str]) -> tuple[int, str, str]:
    """Runs dbt in dbt_project/ with DBT_PROFILES_DIR set, matching
    build_pipeline.py's env/cwd exactly (including --no-partial-parse,
    which that script's comment explains is cheap insurance against a
    stale manifest cache across OSes/paths)."""
    import os

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = "."
    cmd = ["dbt", "--no-partial-parse"] + args
    return run_subprocess(cmd, cwd=DBT_PROJECT_DIR, env=env)


def get_connection(read_only: bool = True):
    """Opens a short-lived DuckDB connection. Callers are responsible for
    closing it (use a `try/finally` or `with`) -- agents never hold a
    connection open across `run()` calls, see module docstring."""
    return duckdb.connect(DB_PATH, read_only=read_only)
