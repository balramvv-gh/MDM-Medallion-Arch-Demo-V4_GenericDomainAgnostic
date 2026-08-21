"""
Matching Agent -- Identity resolution.

Wraps scripts/generate_matches.py (tier-1 exact + tier-2 TF-IDF fuzzy
similarity, metadata-driven off bus_rules.matching_rules/matching_thresholds).

Design note -- why cancel_stale_match_reviews.py runs INSIDE this agent,
not as a separate Steward Agent stage:
scripts/build_pipeline.py's docstring is explicit that
cancel_stale_match_reviews.py "must run strictly between step 2 (silver
just rebuilt) and step 3 (matching runs against that silver) -- running it
any later would let step 3 hold a record whose review should already have
been cancelled." That's a correctness dependency of the Matching stage
itself (a stale Match Review hold must be lifted before matching decides
what's a candidate pair), not a human-in-the-loop action -- no steward
does anything here, it's an automated staleness check. Moving it into a
Steward Agent stage that runs after Matching/Survivorship (as a literal
reading of "run Ingestion, DQ, Matching, Survivorship, Steward in that
order" would imply) would reproduce the exact bug this script's docstring
warns about. See agents/steward_agent.py's docstring and
MCP_Agent_Orchestration_Design.docx's "Design decisions" section for the
full reasoning and what Steward Agent's batch-time role is instead.

Payload: {} (no parameters today -- matches generate_matches.py, which
takes none).
"""
from agents.base import BaseAgent, run_python_script, get_connection


class MatchingAgent(BaseAgent):
    name = "matching"

    def _execute(self, payload: dict) -> tuple[dict, str]:
        returncode, stdout, stderr = run_python_script("cancel_stale_match_reviews.py")
        if returncode != 0:
            raise RuntimeError(f"cancel_stale_match_reviews.py exited {returncode}: {stderr.strip()[-2000:]}")

        returncode, stdout, stderr = run_python_script("generate_matches.py")
        if returncode != 0:
            raise RuntimeError(f"generate_matches.py exited {returncode}: {stderr.strip()[-2000:]}")

        row_counts = self._match_row_counts()
        detail = (
            f"{row_counts.get('match_groups', '?')} match groups, "
            f"{row_counts.get('match_review_candidates', '?')} fuzzy candidates, "
            f"{row_counts.get('match_review_pending', '?')} still-pending Match Review pairs"
        )
        return row_counts, detail

    @staticmethod
    def _match_row_counts() -> dict:
        con = get_connection(read_only=True)
        try:
            counts = {}
            for label, schema, table in [
                ("match_groups", "gold_prep", "match_groups"),
                ("match_review_candidates", "gold_prep", "match_review_candidates"),
            ]:
                exists = con.execute("""
                    SELECT count(*) FROM information_schema.tables
                    WHERE table_schema = ? AND table_name = ?
                """, [schema, table]).fetchone()[0]
                counts[label] = (
                    con.execute(f"SELECT count(*) FROM {schema}.{table}").fetchone()[0]
                    if exists else None
                )

            # main_gold.gold_match_review_queue is a dbt model (rebuilt by the
            # Survivorship Agent's gold `dbt run`, not by this agent) -- it
            # won't exist yet the very first time Matching runs before Gold
            # has ever built, hence the existence check. Live pending status
            # is COALESCE(overrides.status, queue.review_status), same as
            # api/main.py's _PENDING_MATCH_REVIEW_CTE.
            exists = con.execute("""
                SELECT count(*) FROM information_schema.tables
                WHERE table_schema = 'main_gold' AND table_name = 'gold_match_review_queue'
            """).fetchone()[0]
            if exists:
                counts["match_review_pending"] = con.execute("""
                    SELECT count(*)
                    FROM main_gold.gold_match_review_queue q
                    LEFT JOIN stewardship.match_review_overrides o ON o.pair_id = q.pair_id
                    WHERE COALESCE(o.status, q.review_status) IN ('pending', 'in_review')
                """).fetchone()[0]
            else:
                counts["match_review_pending"] = None
            return counts
        finally:
            con.close()


if __name__ == "__main__":
    import json

    result = MatchingAgent().run({})
    print(json.dumps(result.to_dict(), indent=2, default=str))
