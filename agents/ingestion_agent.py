"""
Ingestion Agent -- Source -> Bronze ingestion.

Wraps scripts/load_bronze.py unchanged: same full/incremental
auto-detection off bronze.load_control, same bronze.load_batches
append-only log. This agent adds no new ingestion logic -- it's a thin,
stateless MCP-callable wrapper around the existing, tested script, per
this project's "reuse tested logic, don't reimplement it" approach (see
agents/__init__.py).

Payload:
    {"run_mode": "auto" | "full" | "incremental"}   # default "auto"

"auto"/"incremental" both map to a plain `load_bronze.py` invocation --
the script itself decides full vs. incremental per source from
bronze.load_control (see that script's docstring). "full" maps to
`--force-full`. There is no separate "incremental-only, fail if a control
row is missing" mode, matching current script behavior.
"""
from agents.base import BaseAgent, run_python_script, get_connection


class IngestionAgent(BaseAgent):
    name = "ingestion"

    def _execute(self, payload: dict) -> tuple[dict, str]:
        run_mode = payload.get("run_mode", "auto")
        args = ["--force-full"] if run_mode == "full" else []

        returncode, stdout, stderr = run_python_script("load_bronze.py", args)
        if returncode != 0:
            raise RuntimeError(f"load_bronze.py exited {returncode}: {stderr.strip()[-2000:]}")

        row_counts = self._latest_batch_summary()
        detail = f"bronze load batch {row_counts.get('load_batch_id', '?')[:8]} ({row_counts.get('load_type', '?')})"
        return row_counts, detail

    @staticmethod
    def _latest_batch_summary() -> dict:
        con = get_connection(read_only=True)
        try:
            row = con.execute("""
                SELECT load_batch_id, load_type, sources, row_count, started_at
                FROM bronze.load_batches
                ORDER BY started_at DESC
                LIMIT 1
            """).fetchone()
        finally:
            con.close()
        if row is None:
            return {}
        return {
            "load_batch_id": row[0],
            "load_type": row[1],
            "sources": list(row[2]) if row[2] is not None else [],
            "row_count": row[3],
            "started_at": str(row[4]),
        }


if __name__ == "__main__":
    import json
    import sys

    payload = {"run_mode": sys.argv[1]} if len(sys.argv) > 1 else {}
    result = IngestionAgent().run(payload)
    print(json.dumps(result.to_dict(), indent=2, default=str))
