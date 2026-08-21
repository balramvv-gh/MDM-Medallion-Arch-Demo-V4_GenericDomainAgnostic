from db import run_query, to_records


def _edges_df():
    return run_query("SELECT * FROM main_rules.lineage_edges")


def impact_analysis(layer: str, table: str, column: str):
    """Forward trace: 'if this column changes/breaks, what downstream is affected?'"""
    edges = _edges_df()
    visited = []
    frontier = [(layer, table, column)]
    seen = set()
    while frontier:
        cur_layer, cur_table, cur_col = frontier.pop(0)
        key = (cur_layer, cur_table, cur_col)
        if key in seen:
            continue
        seen.add(key)
        matches = edges[
            (edges.from_layer == cur_layer)
            & (edges.from_table == cur_table)
            & ((edges.from_column == cur_col) | (edges.from_column == "*"))
        ]
        for _, row in matches.iterrows():
            visited.append({
                "from": f"{row.from_layer}.{row.from_table}.{row.from_column}",
                "to": f"{row.to_layer}.{row.to_table}.{row.to_column}",
                "rule_id": row.transform_rule_id,
                "description": row.transform_description,
            })
            frontier.append((row.to_layer, row.to_table, row.to_column))
    return visited


def lineage_trace(layer: str, table: str, column: str):
    """Backward trace: 'where did this column's data actually come from?'"""
    edges = _edges_df()
    visited = []
    frontier = [(layer, table, column)]
    seen = set()
    while frontier:
        cur_layer, cur_table, cur_col = frontier.pop(0)
        key = (cur_layer, cur_table, cur_col)
        if key in seen:
            continue
        seen.add(key)
        matches = edges[
            (edges.to_layer == cur_layer)
            & (edges.to_table == cur_table)
            & ((edges.to_column == cur_col) | (edges.to_column == "*"))
        ]
        for _, row in matches.iterrows():
            visited.append({
                "from": f"{row.from_layer}.{row.from_table}.{row.from_column}",
                "to": f"{row.to_layer}.{row.to_table}.{row.to_column}",
                "rule_id": row.transform_rule_id,
                "description": row.transform_description,
            })
            frontier.append((row.from_layer, row.from_table, row.from_column))
    return visited


def trace_golden_record(golden_id: str):
    """Record-level lineage: for a specific golden record, show every contributing
    source record plus the column-level transformation path that produced it.

    Phase 4 (domain-agnostic): bronze is now one shared table
    (bronze.raw_records, domain_key + source_system + source_record_id +
    attributes JSON) instead of a table per source system, so this no longer
    branches on source_system to pick a raw table name -- one query, scoped
    by domain_key (read off the crosswalk row itself, so callers don't have
    to already know it from the golden_id) plus source_system/
    source_record_id, covers every source. gold_crosswalk.winning_columns was
    renamed winning_attributes in Phase 3 (dbt_project/models/gold/
    gold_crosswalk.sql).
    """
    crosswalk = run_query(
        "SELECT * FROM main_gold.gold_crosswalk WHERE golden_id = ?", [golden_id]
    )
    if crosswalk.empty:
        return None

    domain_key = crosswalk.iloc[0]["domain_key"]
    contributing_sources = []
    for _, row in crosswalk.iterrows():
        raw = run_query(
            "SELECT * FROM bronze.raw_records WHERE domain_key = ? AND source_system = ? AND source_record_id = ?",
            [domain_key, row.source_system, row.source_record_id],
        )
        winning_attributes = list(row.winning_attributes) if row.winning_attributes is not None else []
        contributing_sources.append({
            "source_system": row.source_system,
            "source_record_id": row.source_record_id,
            "match_confidence_score": float(row.match_confidence_score),
            "is_survivor_record": bool(row.is_survivor_record),
            "winning_attributes": winning_attributes,
            "raw_bronze_data": to_records(raw)[0] if not raw.empty else None,
        })

    return {
        "golden_id": golden_id,
        "domain_key": domain_key,
        "contributing_sources": contributing_sources,
        "note": "Gold record is an attribute-level survivorship merge of the contributing "
                "sources above -- each source's winning_attributes shows which specific gold "
                "attributes it won (Data Governance > Rules Configuration > Survivorship "
                "Rules). See /lineage/impact for full column-level transformation rules.",
    }
