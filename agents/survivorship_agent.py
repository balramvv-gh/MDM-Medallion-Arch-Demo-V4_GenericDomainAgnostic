"""
Survivorship Agent -- Survivorship.

Wraps the gold-layer dbt run (`dbt run --select gold.*`), where
attribute-level survivorship actually happens (bus_rules.survivorship_rules,
applied in gold_survivorship_winners.sql / gold_crosswalk.sql), followed by
scripts/audit_pipeline_diff.py.

Design note -- why audit_pipeline_diff.py runs INSIDE this agent, not as a
separate stage: scripts/build_pipeline.py's docstring says it "must run
immediately after step 4 [the gold dbt run], before any other write
touches main_gold.golden_records" -- it diffs the freshly rebuilt gold
table against its own snapshot of the previous build to log
creates/updates/logical deletes to the audit trail. That's specifically
auditing THIS stage's own output, so it stays part of this agent rather
than becoming a sixth pipeline stage with its own ordering constraints to
track separately.

Payload: {} (no parameters today).
"""
import re

from agents.base import BaseAgent, run_dbt, run_python_script, get_connection

_DIFF_SUMMARY_RE = re.compile(
    r"Audit diff complete: (\d+) created, (\d+) updated, (\d+) logically deleted \(of (\d+) current gold records\)"
)


class SurvivorshipAgent(BaseAgent):
    name = "survivorship"

    def _execute(self, payload: dict) -> tuple[dict, str]:
        returncode, stdout, stderr = run_dbt(["run", "--select", "gold.*"])
        if returncode != 0:
            raise RuntimeError(f"dbt run (gold) exited {returncode}: {stderr.strip()[-2000:]}")

        returncode, stdout, stderr = run_python_script("audit_pipeline_diff.py")
        if returncode != 0:
            raise RuntimeError(f"audit_pipeline_diff.py exited {returncode}: {stderr.strip()[-2000:]}")

        row_counts = self._parse_diff_summary(stdout)
        if not row_counts:
            row_counts = {"gold_records": self._gold_row_count()}
        detail = (
            f"{row_counts.get('created', '?')} created, {row_counts.get('updated', '?')} updated, "
            f"{row_counts.get('logically_deleted', '?')} logically deleted "
            f"(of {row_counts.get('gold_records', '?')} current gold records)"
        )
        return row_counts, detail

    @staticmethod
    def _parse_diff_summary(stdout: str) -> dict:
        match = _DIFF_SUMMARY_RE.search(stdout or "")
        if not match:
            return {}
        created, updated, deleted, total = (int(g) for g in match.groups())
        return {
            "created": created,
            "updated": updated,
            "logically_deleted": deleted,
            "gold_records": total,
        }

    @staticmethod
    def _gold_row_count():
        """Fallback only -- normally _parse_diff_summary() already extracted
        the total from audit_pipeline_diff.py's own stdout. main_gold.golden_records
        is the real, current table (domain-agnostic since Phase 3, replacing
        the old fixed gold_customers table)."""
        con = get_connection(read_only=True)
        try:
            exists = con.execute("""
                SELECT count(*) FROM information_schema.tables
                WHERE table_schema = 'main_gold' AND table_name = 'golden_records'
            """).fetchone()[0]
            return con.execute("SELECT count(*) FROM main_gold.golden_records").fetchone()[0] if exists else None
        finally:
            con.close()


if __name__ == "__main__":
    import json

    result = SurvivorshipAgent().run({})
    print(json.dumps(result.to_dict(), indent=2, default=str))
