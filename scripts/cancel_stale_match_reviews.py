"""
Batch-path trigger for cancelling a stale Match Review pair: detects when an
updated version of a source record (same source_system+source_record_id) has
landed in main_silver.silver_customers since the last time this script ran --
which, for the batch pipeline, means since the last `dbt run --exclude gold.*`
rebuilt silver from bronze -- and cancels any Match Review pair that record is
part of and that is still open ('pending'/'in_review', or 'confirmed' but not
yet actually merged into gold).

Runs as its own step in scripts/build_pipeline.py, between the silver dbt run
and scripts/generate_matches.py -- it has to run before generate_matches.py so
a freshly-cancelled record is treated as fully eligible (not held) in that
same build's matching pass. See scripts/generate_matches.py's docstring
("Match Review hold") for the other half of this lifecycle, and
api/reprocessing.py's _cancel_stale_match_reviews for this same check's
real-time-path equivalent (triggered by a steward correction instead of a
batch rebuild) -- kept as a separate, parallel implementation here rather than
a shared import, same "api/ and scripts/ each stay self-contained, logic kept
deliberately in lockstep with a comment" convention already used for
TRANSFORM_FUNCTIONS/PY_TRANSFORMS/SQL_TRANSFORMS between this directory and
api/reprocessing.py.

Detection: unlike gold (audit.gold_customers_snapshot / audit_pipeline_diff.py),
silver has no other diff mechanism -- dbt CREATE OR REPLACEs the whole table
every run. stewardship.silver_customers_snapshot (source_system,
source_record_id, source_modified_date) is this script's own point-in-time
snapshot, refreshed at the end of every run. A key whose source_modified_date
differs from the snapshot (or is new since the snapshot) is a changed source
record; only a key that was ALREADY in the snapshot is checked for a stale
review, since a review can never reference a source key that didn't exist yet
when it was generated.

Usage: python scripts/cancel_stale_match_reviews.py
(scripts/build_pipeline.py runs this automatically as part of the full build.)
"""
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb

DB_PATH = str(Path(__file__).resolve().parent.parent / "mdm_demo.duckdb")


def _ensure_tables(con):
    """Same IF-NOT-EXISTS duplication convention as generate_matches.py's
    _ensure_match_review_tables -- whichever of api/db.py, generate_matches.py,
    or this script runs first on a fresh DB, all three definitions must stay
    identical."""
    con.execute("CREATE SCHEMA IF NOT EXISTS stewardship;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS stewardship.match_review_overrides (
            pair_id VARCHAR PRIMARY KEY,
            status VARCHAR,
            steward_note VARCHAR,
            updated_ts TIMESTAMP DEFAULT current_timestamp
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS stewardship.match_review_log (
            log_id VARCHAR,
            pair_id VARCHAR,
            action VARCHAR,
            steward_note VARCHAR,
            created_ts TIMESTAMP DEFAULT current_timestamp
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS stewardship.silver_customers_snapshot (
            source_system VARCHAR,
            source_record_id VARCHAR,
            source_modified_date TIMESTAMP,
            PRIMARY KEY (source_system, source_record_id)
        );
    """)


def _table_exists(con, schema, table):
    row = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [schema, table],
    ).fetchone()
    return row[0] > 0


def _cancel_stale_match_reviews_for_key(con, source_system, source_record_id, now) -> list:
    """Same logic as api/reprocessing.py's _cancel_stale_match_reviews, against
    a raw duckdb connection instead of api/db.py's run_query/run_write -- see
    module docstring for why this is a parallel implementation, not a shared
    import. Also mirrors that function's fallback: a 'confirmed' pair's
    identity can drop out of gold_prep.match_review_candidates on a later run
    (real similarity no longer in the review band, or a blocking_key field
    changed) while it's still unmerged in stewardship.match_review_overrides
    -- fall back to the dbt-published main_gold.gold_match_review_queue for
    identity in that case."""
    import pandas as pd

    candidates = pd.DataFrame()
    if _table_exists(con, "gold_prep", "match_review_candidates"):
        candidates = con.execute("""
            SELECT pair_id, source_system_a, source_record_id_a, source_system_b, source_record_id_b
            FROM gold_prep.match_review_candidates
            WHERE (source_system_a = ? AND source_record_id_a = ?)
               OR (source_system_b = ? AND source_record_id_b = ?)
        """, [source_system, source_record_id, source_system, source_record_id]).fetchdf()

    if _table_exists(con, "main_gold", "gold_match_review_queue"):
        queue_candidates = con.execute("""
            SELECT pair_id, source_system_a, source_record_id_a, source_system_b, source_record_id_b
            FROM main_gold.gold_match_review_queue
            WHERE (source_system_a = ? AND source_record_id_a = ?)
               OR (source_system_b = ? AND source_record_id_b = ?)
        """, [source_system, source_record_id, source_system, source_record_id]).fetchdf()
        if not queue_candidates.empty:
            known_pair_ids = set(candidates["pair_id"]) if not candidates.empty else set()
            extra = queue_candidates[~queue_candidates["pair_id"].isin(known_pair_ids)]
            candidates = pd.concat([candidates, extra], ignore_index=True) if not extra.empty else candidates

    if candidates.empty:
        return []

    overrides = con.execute(
        "SELECT pair_id, status FROM stewardship.match_review_overrides WHERE pair_id IN ("
        + ",".join(["?"] * len(candidates)) + ")",
        list(candidates["pair_id"]),
    ).fetchdf()
    status_by_pair = dict(zip(overrides["pair_id"], overrides["status"])) if not overrides.empty else {}

    note = (
        f"Updated version of this record ({source_system}-{source_record_id}) "
        f"received on {now.strftime('%m/%d/%Y')}"
    )
    cancelled = []
    for _, row in candidates.iterrows():
        pair_id = row["pair_id"]
        status = status_by_pair.get(pair_id, "pending")  # no override row yet == still pending

        if status in ("rejected", "cancelled"):
            continue  # already terminal, nothing to do

        if status == "confirmed":
            merged = con.execute("""
                SELECT 1
                FROM main_gold.gold_crosswalk a
                JOIN main_gold.gold_crosswalk b ON a.golden_id = b.golden_id
                WHERE a.source_system = ? AND a.source_record_id = ?
                  AND b.source_system = ? AND b.source_record_id = ?
            """, [row["source_system_a"], row["source_record_id_a"],
                  row["source_system_b"], row["source_record_id_b"]]).fetchdf()
            if not merged.empty:
                continue  # already realized -- nothing to cancel

        con.execute("""
            INSERT OR REPLACE INTO stewardship.match_review_overrides (pair_id, status, steward_note)
            VALUES (?, 'cancelled', ?)
        """, [pair_id, note])
        con.execute("""
            INSERT INTO stewardship.match_review_log (log_id, pair_id, action, steward_note)
            VALUES (?, ?, 'cancelled', ?)
        """, [str(uuid.uuid4()), pair_id, note])
        cancelled.append({"pair_id": pair_id, "note": note})

    return cancelled


def main():
    con = duckdb.connect(DB_PATH)
    _ensure_tables(con)

    if not _table_exists(con, "main_silver", "silver_customers"):
        print("main_silver.silver_customers doesn't exist yet -- nothing to diff, skipping.")
        con.close()
        return

    current = con.execute(
        "SELECT source_system, source_record_id, source_modified_date FROM main_silver.silver_customers"
    ).fetchdf()
    prior = con.execute(
        "SELECT source_system, source_record_id, source_modified_date FROM stewardship.silver_customers_snapshot"
    ).fetchdf()
    prior_by_key = {
        (r["source_system"], r["source_record_id"]): r["source_modified_date"]
        for _, r in prior.iterrows()
    }

    now = datetime.now(timezone.utc)
    changed_keys = []
    for _, row in current.iterrows():
        key = (row["source_system"], row["source_record_id"])
        if key in prior_by_key and prior_by_key[key] != row["source_modified_date"]:
            changed_keys.append(key)

    total_cancelled = []
    for source_system, source_record_id in changed_keys:
        total_cancelled += _cancel_stale_match_reviews_for_key(con, source_system, source_record_id, now)

    # Refresh the snapshot for next run.
    con.execute("DELETE FROM stewardship.silver_customers_snapshot;")
    con.executemany(
        "INSERT INTO stewardship.silver_customers_snapshot VALUES (?, ?, ?)",
        [(r["source_system"], r["source_record_id"], r["source_modified_date"]) for _, r in current.iterrows()],
    )

    con.close()

    print(f"Stale Match Review check complete: {len(current)} silver records, {len(changed_keys)} changed since last snapshot")
    print(f"  match reviews cancelled: {len(total_cancelled)}")
    for c in total_cancelled:
        print(f"    {c['pair_id']}: {c['note']}")


if __name__ == "__main__":
    main()
