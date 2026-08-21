"""
DQ Agent -- Profiling, validation, standardization.

Wraps the silver-layer dbt run (`dbt run --exclude gold.*`), which is where
metadata-driven cleansing/standardization/validation actually happens today
(bus_rules.column_rules, applied in stg_crm_customers.sql/stg_erp_customers.sql;
records failing validation route to exceptions_queue instead of
silver_customers). No validation logic is reimplemented here -- this agent
only decides --full-refresh the same way scripts/build_pipeline.py does
(reads bronze.load_batches for the most recent batch's load_type) and
reports post-run exception/silver row counts.

Payload:
    {"full_refresh": true | false | null}   # null (default) = auto-detect,
                                              # same as build_pipeline.py
"""
from agents.base import BaseAgent, run_dbt, get_connection


class DQAgent(BaseAgent):
    name = "dq"

    def _execute(self, payload: dict) -> tuple[dict, str]:
        full_refresh = payload.get("full_refresh")
        if full_refresh is None:
            full_refresh = self._latest_bronze_load_is_full()

        cmd = ["run", "--exclude", "gold.*"]
        if full_refresh:
            cmd.append("--full-refresh")

        returncode, stdout, stderr = run_dbt(cmd)
        if returncode != 0:
            raise RuntimeError(f"dbt run (silver) exited {returncode}: {stderr.strip()[-2000:]}")

        row_counts = self._silver_row_counts()
        detail = (
            f"silver rebuilt (full_refresh={full_refresh}); "
            f"{row_counts.get('silver_customers', '?')} silver rows, "
            f"{row_counts.get('exceptions_queued', '?')} exceptions queued this rebuild"
        )
        return row_counts, detail

    @staticmethod
    def _latest_bronze_load_is_full() -> bool:
        """Same logic as scripts/build_pipeline.py's
        _latest_bronze_load_is_full() -- duplicated rather than imported so
        this agent has no import-time dependency on the scripts/ package
        (agents/ only shells out to scripts/, per this project's existing
        "Python loads, downstream treats it as a source" separation)."""
        con = get_connection(read_only=True)
        try:
            exists = con.execute("""
                SELECT count(*) FROM information_schema.tables
                WHERE table_schema = 'bronze' AND table_name = 'load_batches'
            """).fetchone()[0]
            if not exists:
                return True
            row = con.execute(
                "SELECT load_type FROM bronze.load_batches ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
            return row is None or row[0] == "FULL"
        finally:
            con.close()

    @staticmethod
    def _silver_row_counts() -> dict:
        """exceptions_queued is a plain count of main_silver.exceptions_queue
        after this rebuild -- every row the dbt model produces starts
        'open' (see exceptions_queue.sql's remediation_status literal); the
        LIVE open/in_review/resolved/rejected status stewards actually see
        lives in exception_status_overrides (API layer), not in this model,
        so this count is "records this rebuild flagged as invalid", not
        "currently outstanding in the steward queue". The Monitoring Agent
        is the one that should join against exception_status_overrides for
        a true live open count -- see agents/monitoring_agent.py."""
        con = get_connection(read_only=True)
        try:
            counts = {}
            for label, schema, table in [
                ("silver_customers", "main_silver", "silver_customers"),
                ("exceptions_queued", "main_silver", "exceptions_queue"),
            ]:
                exists = con.execute("""
                    SELECT count(*) FROM information_schema.tables
                    WHERE table_schema = ? AND table_name = ?
                """, [schema, table]).fetchone()[0]
                counts[label] = (
                    con.execute(f"SELECT count(*) FROM {schema}.{table}").fetchone()[0]
                    if exists else None
                )
            return counts
        finally:
            con.close()


if __name__ == "__main__":
    import json

    result = DQAgent().run({})
    print(json.dumps(result.to_dict(), indent=2, default=str))
