"""
Audit-trail diff step for the batch pipeline.

Runs after `dbt run --select gold.*` (see scripts/build_pipeline.py, final
step). Compares the just-rebuilt main_gold.golden_records against a snapshot
of the previous build (audit.golden_records_snapshot -- a table in a schema
dbt never touches, so it survives every `dbt run`'s CREATE OR REPLACE of the
gold schema), and writes append-only rows to audit.audit_trail for every
golden_id that:

  - is new since the last snapshot                          -> 'created'
  - existed before and has one or more changed tracked
    fields (survivorship/match recompute)                   -> 'updated'
  - existed before but is no longer produced by this build   -> 'logically_deleted'

Then refreshes the snapshot to the current state.

Phase 3 (domain-agnostic): golden_records.attributes is a JSON bag whose key
set varies by domain (governance.domain_attributes), so this script no
longer hardcodes a Customer field list (the old TRACKED_FIELDS). Tracked
attribute fields are simply the union of keys present in the record's
old and/or new `attributes` JSON -- diffing whatever keys actually exist
requires no domain metadata lookup at all, and adapts automatically if a
domain's attribute set changes later. FIXED_TRACKED_FIELDS (the three
non-attribute, survivorship-summary columns) stays a short constant since
those are structural golden_records columns, not domain attributes.

This is the ONLY place that captures pipeline-driven ("systemic") gold-layer
changes -- api/audit.py's log_creation/log_update cover the two live, in-app
write paths (portal manual edit, steward real-time reprocessing). This script
duplicates the audit table DDL rather than importing api/audit.py or api/db.py,
matching this project's existing convention for standalone scripts owning
their own idempotent DDL (see scripts/generate_matches.py's docstring on
stewardship.match_review_* -- same rationale applies here).

Known limitation (consistent with reprocessing.py's documented divergence): a
golden_id created via real-time reprocessing can be renumbered by the next
full `dbt run` (dbt's match-group numbering is a fresh dense_rank over the
complete silver set on every run, per domain). When that happens, this diff
logs the old number as 'logically_deleted' and the new number as 'created' --
even though it's the same underlying record. That's an accepted tradeoff of
this demo's golden ID numbering scheme, not a bug in the diff logic itself.

Usage: python scripts/audit_pipeline_diff.py
(called automatically as the final step of scripts/build_pipeline.py)
"""
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb

DB_PATH = str(Path(__file__).resolve().parent.parent / "mdm_demo.duckdb")

CHANGED_BY = "system:dbt_pipeline"
CHANGED_BY_LABEL = "Batch Pipeline (dbt run)"


def _ensure_audit_tables(con):
    """Same idempotent-DDL pattern as api/db.py's _ensure_audit_tables --
    whichever of this script or the API runs first creates them, both must
    stay column-for-column identical."""
    con.execute("CREATE SCHEMA IF NOT EXISTS audit;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS audit.audit_trail (
            audit_id VARCHAR PRIMARY KEY,
            golden_id VARCHAR,
            change_batch_id VARCHAR,
            event_ts TIMESTAMP,
            event_type VARCHAR,
            event_source VARCHAR,
            changed_by VARCHAR,
            changed_by_label VARCHAR,
            field_name VARCHAR,
            old_value VARCHAR,
            new_value VARCHAR,
            change_reason VARCHAR,
            related_exception_id VARCHAR
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS audit.golden_records_snapshot (
            golden_id VARCHAR PRIMARY KEY,
            domain_key VARCHAR,
            attributes JSON,
            source_system_count INTEGER,
            survivor_source_system VARCHAR,
            survivor_source_record_id VARCHAR
        );
    """)


def _s(v):
    return None if v is None else str(v)


def _record_dict(domain_key, attributes_json, source_system_count, survivor_source_system, survivor_source_record_id):
    rec = dict(json.loads(attributes_json)) if attributes_json else {}
    rec["source_system_count"] = source_system_count
    rec["survivor_source_system"] = survivor_source_system
    rec["survivor_source_record_id"] = survivor_source_record_id
    return rec


def main():
    con = duckdb.connect(DB_PATH)
    _ensure_audit_tables(con)

    snapshot_was_empty = con.execute("SELECT COUNT(*) FROM audit.golden_records_snapshot").fetchone()[0] == 0

    prev = {}
    for golden_id, domain_key, attrs, ssc, survivor_ss, survivor_sid in con.execute(
        "SELECT golden_id, domain_key, attributes, source_system_count, "
        "survivor_source_system, survivor_source_record_id FROM audit.golden_records_snapshot"
    ).fetchall():
        prev[golden_id] = (domain_key, _record_dict(domain_key, attrs, ssc, survivor_ss, survivor_sid))

    curr = {}
    for golden_id, domain_key, attrs, ssc, survivor_ss, survivor_sid in con.execute(
        "SELECT golden_id, domain_key, attributes, source_system_count, "
        "survivor_source_system, survivor_source_record_id FROM main_gold.golden_records"
    ).fetchall():
        curr[golden_id] = (domain_key, _record_dict(domain_key, attrs, ssc, survivor_ss, survivor_sid))

    now = datetime.now(timezone.utc)
    audit_rows = []
    created_count = updated_count = deleted_count = 0

    for golden_id, (domain_key, rec) in curr.items():
        if golden_id not in prev:
            batch_id = str(uuid.uuid4())
            reason = (
                "Existing gold record captured on audit trail initialization "
                "(pre-dates audit tracking; true creation time unknown)"
                if snapshot_was_empty else
                "Initial gold record created by pipeline batch run"
            )
            for field in sorted(rec.keys()):
                val = _s(rec.get(field))
                if val is None:
                    continue
                audit_rows.append((str(uuid.uuid4()), golden_id, batch_id, now, "created", "pipeline_batch",
                                    CHANGED_BY, CHANGED_BY_LABEL, field, None, val, reason, None))
            created_count += 1
        else:
            _, old = prev[golden_id]
            batch_id = str(uuid.uuid4())
            changed_any = False
            all_fields = sorted(set(old.keys()) | set(rec.keys()))
            for field in all_fields:
                old_val, new_val = _s(old.get(field)), _s(rec.get(field))
                if old_val == new_val:
                    continue
                changed_any = True
                audit_rows.append((str(uuid.uuid4()), golden_id, batch_id, now, "updated", "pipeline_batch",
                                    CHANGED_BY, CHANGED_BY_LABEL, field, old_val, new_val,
                                    "Recomputed via pipeline batch run (match/survivorship recompute)", None))
            if changed_any:
                updated_count += 1

    for golden_id in prev:
        if golden_id not in curr:
            audit_rows.append((str(uuid.uuid4()), golden_id, str(uuid.uuid4()), now, "logically_deleted",
                                "pipeline_batch", CHANGED_BY, CHANGED_BY_LABEL, None, None, None,
                                "Golden record no longer produced by the pipeline (contributing source "
                                "record(s) removed, or record merged into a different golden ID during "
                                "re-matching)", None))
            deleted_count += 1

    if audit_rows:
        con.executemany("""
            INSERT INTO audit.audit_trail
                (audit_id, golden_id, change_batch_id, event_ts, event_type, event_source,
                 changed_by, changed_by_label, field_name, old_value, new_value, change_reason,
                 related_exception_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, audit_rows)

    con.execute("DELETE FROM audit.golden_records_snapshot;")
    con.execute("""
        INSERT INTO audit.golden_records_snapshot
        SELECT golden_id, domain_key, attributes, source_system_count,
               survivor_source_system, survivor_source_record_id
        FROM main_gold.golden_records
    """)

    con.close()
    print(f"Audit diff complete: {created_count} created, {updated_count} updated, "
          f"{deleted_count} logically deleted (of {len(curr)} current gold records).")


if __name__ == "__main__":
    main()
