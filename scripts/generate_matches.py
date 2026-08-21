"""
Match/merge step: embedding-based duplicate detection over the silver layer.

Runs BETWEEN silver and gold in the build sequence (silver.records must
already exist; the gold layer reads this script's output as a dbt source --
see dbt_project/models/gold/gold_match_candidates.sql):

    dbt seed
    dbt run --exclude gold.*
    python scripts/generate_matches.py
    dbt run --select gold.*

(scripts/build_pipeline.py runs all four steps in order, cross-platform.)

Why this lives outside dbt: candidate embeddings require a Python ML library
(scikit-learn) that plain dbt-duckdb SQL can't produce. Everything downstream
of the embeddings (blocking, similarity scoring, clustering) is also done here
for a single coherent matching pass; the gold dbt models just consume the
result as a source table, the same pattern already used for the bronze layer
(scripts/load_bronze.py Python-loads bronze; dbt treats it as a source).

Phase 3 (domain-agnostic): this script now runs its matching algorithm once
PER DOMAIN, discovered generically from which domain_keys have at least one
active bus_rules.matching_thresholds row (see _domains_with_matching_config)
-- exactly the same "metadata's presence is the activation signal, not a
hardcoded name" pattern dbt_project/macros/active_domain_sources.sql already
uses for silver. Today only 'customer' has any matching_rules/
matching_thresholds rows, so only 'customer' runs -- a second live domain is
onboarded purely by adding its own rows to those two tables (now domain_key-
scoped, see scripts/migrate_matching_rules_to_domain.py), never by changing
this file. gold_prep.match_groups/match_edges/match_review_candidates are
shared tables across every domain (like bronze.raw_records) -- each domain's
run DELETEs and re-inserts only its own domain_key's rows, never another
domain's.

Matching is metadata-driven, not hardcoded. Tier definitions and thresholds
live in bus_rules.matching_thresholds; the fields/attributes each tier
actually operates on live in bus_rules.matching_rules, a child table keyed by
tier_id, now referencing domain_attributes.attribute_key (matching_rules'
own source_column is kept for reference/history, not dropped -- same
additive-migration convention as bus_rules.column_rules). Both are DB-native
tables (not dbt seeds) maintained via the Rules Configuration screen's
maker-checker workflow.

Two-tier matching strategy, as configured today in the seeds (more tiers can
be added by adding rows -- each tier's match_method must be either 'exact'
or 'fuzzy_tfidf_cosine' for this script to know how to evaluate it):

  Tier 1 (match_method='exact'): two source records are the same customer if
    they agree, after that rule's transform_function is applied, on ANY
    active exact_match_field rule for the tier (today: normalized email OR
    normalized phone -- matching_rules.csv rows MR001/MR002). Confidence is
    always that tier's auto_merge_threshold (1.00).

  Tier 2 (match_method='fuzzy_tfidf_cosine'): candidate pairs are generated
    via blocking (grouped by the tier's blocking_key rule(s) -- avoids O(n^2)
    comparisons at scale; today a single attribute, state_code, but multiple
    blocking_key rows compose into a multi-attribute key), then scored by
    cosine similarity over TF-IDF character n-gram vectors built from the
    tier's similarity_text_field attributes, concatenated in rule_order
    (today: first_name, last_name, address_line1, address_line2, city --
    matching_rules.csv rows MR003-MR007). Deliberately excludes any attribute
    not listed as a similarity_text_field for the tier -- email/phone are
    intentionally absent (those are exactly the fields tier 2 exists to
    catch disagreement on; including them would dilute the name/address
    signal with noise).
      - similarity >= auto_merge_threshold -> auto-merge, confidence = similarity
      - review_lower_threshold <= similarity < auto_merge_threshold -> NOT
        auto-merged; written to the Match Review queue for a data steward to
        confirm or reject. A prior steward decision
        (stewardship.match_review_overrides) is honored: 'confirmed' pairs
        are unioned as if tier-2-high, 'rejected' pairs are permanently
        excluded.
      - similarity < review_lower_threshold -> not a candidate at all.

Thresholds (0.80 / 0.35 today, see matching_thresholds.csv rows MT002) were
calibrated empirically against this project's synthetic data generator
(data/generate_source_data.py), which seeds 12 deliberate fuzzy-duplicate
pairs (different email+phone, similar name/address) specifically so this
tier has real positive cases to find, and against every same-state
non-duplicate pair as the negative class. At that calibration: true fuzzy
pairs scored 0.71-0.95, unrelated same-state pairs scored at most 0.19 -- a
wide, clean margin. Re-run the calibration check (see PROJECT_KNOWLEDGE.md)
if the data generator's population or field construction changes materially,
or if matching_rules.csv's similarity_text_field list changes.

The TF-IDF vectorizer's shape (char_wb analyzer, 2-4 char n-grams) is a fixed
algorithm parameter, not tier metadata -- it applies identically to every
fuzzy_tfidf_cosine tier, so it stays a code constant (TFIDF_ANALYZER /
TFIDF_NGRAM_RANGE below) rather than a seeded value.

Clustering: Union-Find (path compression + union by rank) over every edge that
should auto-merge (tier-1 exact, tier-2 >= auto_merge_threshold, tier-2
confirmed by a steward). Borderline non-overridden pairs are NOT unioned --
they surface in the review queue instead. match_group_id is assigned by
sorting clusters on their lowest (source_system, source_record_id) member
WITHIN a domain (match_group_id is only ever compared to another edge/candidate
row of the same domain_key downstream, so reusing small integers per domain
is fine -- gold_customers.sql's golden_id is derived from (domain_key,
match_group_id) together, not match_group_id alone), so IDs are stable
across reruns as long as the underlying silver population doesn't change.

Known simplification: this batch step is the only place fuzzy
(match_method='fuzzy_tfidf_cosine') matching runs. The real-time reprocessing
path (api/reprocessing.py, triggered when a steward resolves a validation
exception) also reads its match fields from bus_rules.matching_rules now,
but only ever evaluates the 'exact' tier -- extending it to fuzzy tiers would
mean fitting a TF-IDF vectorizer per API request, which is a real
latency/complexity cost for a demo-scoped feature. This mirrors this
project's existing documented divergence between real-time reprocessing and
the full batch pipeline.

Match Review hold: a silver record that is a suspect match with another
record (i.e. is currently one of the two source records on a Match Review
pair whose stewardship.match_review_overrides status is 'pending' or
'in_review' -- still undecided) sits out of this run's tier-1 and tier-2
candidate generation ENTIRELY -- it does not get compared against, or form a
new edge/candidate with, any other silver record, including a third record
unrelated to its pending pair. This prevents a record from being auto-merged
into gold via a different path (or drawn into an unrelated new suspect-match)
while a steward's decision on its identity is still outstanding. Its own
already-flagged pending pair is carried forward untouched (frozen
similarity_score/blocking_key, from gold_prep.match_review_candidates as it
existed at the start of this run) into the rebuilt table, so the queue item
stays visible and stable instead of disappearing.

Deliberately NOT held: 'confirmed' pairs. This script is what actually
realizes a confirmed decision (unions the pair via fuzzy_confirmed_edges,
below) -- excluding them from this run would mean a confirmed pair could
never merge. Only 'pending'/'in_review' (undecided) triggers the hold.
'rejected' and 'cancelled' pairs are fully terminal; their records already
return to normal matching today (only that specific pair_id is excluded from
resurfacing).

A held record's hold lifts the moment its pair resolves -- a steward
rejecting it (status -> 'rejected'), or an updated version of one of its two
source records landing in silver, which cancels the stale review (status ->
'cancelled', see stewardship.match_review_overrides' docstring in api/db.py,
api/reprocessing.py's _cancel_stale_match_reviews, and
scripts/cancel_stale_match_reviews.py for the batch-path trigger).
"""
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

DB_PATH = str(Path(__file__).resolve().parent.parent / "mdm_demo.duckdb")

# Fixed algorithm shape for every fuzzy_tfidf_cosine tier -- not tier metadata
# (see docstring above).
TFIDF_ANALYZER = "char_wb"
TFIDF_NGRAM_RANGE = (2, 4)

# Registry of the transform_function values matching_rules.csv rows may use.
# 'none' is the identity transform; add new named transforms here (and to the
# matching seed data) rather than hardcoding field-specific logic elsewhere.
TRANSFORM_FUNCTIONS = {
    "none": lambda v: v,
    "normalize_email": lambda v: (v or "").strip().lower(),
    "normalize_phone": lambda v: re.sub(r"[^0-9]", "", v or ""),
}


def _apply_transform(transform_function, value):
    fn = TRANSFORM_FUNCTIONS.get(transform_function or "none", TRANSFORM_FUNCTIONS["none"])
    return fn(value)


def _combined_text(row, similarity_rules):
    """Concatenates a tier's similarity_text_field attributes, in rule_order,
    after each attribute's transform_function is applied."""
    parts = []
    for rule in similarity_rules:
        val = _apply_transform(rule["transform_function"], row.get(rule["attribute_key"]))
        parts.append(str(val) if val else "")
    return " ".join(p for p in parts if p)


def _blocking_key(row, blocking_rules):
    """Composes a tier's blocking_key rule(s), in rule_order, into a single
    grouping key. Multiple rows compose into a multi-attribute composite key."""
    if not blocking_rules:
        return None
    parts = []
    for rule in blocking_rules:
        val = _apply_transform(rule["transform_function"], row.get(rule["attribute_key"]))
        parts.append(str(val) if val is not None else "")
    return "||".join(parts)


def _pair_id(a, b):
    """Deterministic id for an unordered pair of (source_system, source_record_id),
    stable across reruns so steward review decisions stay attached to the same pair
    even after this table is rebuilt from a fresh pipeline run."""
    key_a, key_b = f"{a[0]}:{a[1]}", f"{b[0]}:{b[1]}"
    ordered = sorted([key_a, key_b])
    return hashlib.md5("|".join(ordered).encode()).hexdigest()


class UnionFind:
    def __init__(self, items):
        self.parent = {i: i for i in items}
        self.rank = {i: 0 for i in items}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1


def _ensure_match_review_tables(con):
    """Same IF-NOT-EXISTS pattern as api/db.py's stewardship tables -- whichever
    of this script or the API runs first creates them, both stay consistent."""
    con.execute("CREATE SCHEMA IF NOT EXISTS stewardship;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS stewardship.match_review_overrides (
            pair_id VARCHAR PRIMARY KEY,
            status VARCHAR,             -- 'confirmed' | 'rejected'
            steward_note VARCHAR,
            updated_ts TIMESTAMP DEFAULT current_timestamp
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS stewardship.match_review_log (
            log_id VARCHAR,
            pair_id VARCHAR,
            action VARCHAR,             -- 'confirmed' | 'rejected'
            steward_note VARCHAR,
            created_ts TIMESTAMP DEFAULT current_timestamp
        );
    """)


def _ensure_gold_prep_tables(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS gold_prep;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS gold_prep.match_groups (
            domain_key VARCHAR, source_system VARCHAR, source_record_id VARCHAR, match_group_id INTEGER
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS gold_prep.match_edges (
            domain_key VARCHAR,
            source_system_a VARCHAR, source_record_id_a VARCHAR,
            source_system_b VARCHAR, source_record_id_b VARCHAR,
            match_type VARCHAR, confidence DOUBLE, created_ts TIMESTAMP
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS gold_prep.match_review_candidates (
            domain_key VARCHAR,
            pair_id VARCHAR, source_system_a VARCHAR, source_record_id_a VARCHAR,
            source_system_b VARCHAR, source_record_id_b VARCHAR,
            similarity_score DOUBLE, blocking_key VARCHAR, generated_ts TIMESTAMP
        );
    """)


def _domains_with_matching_config(con):
    """The generic activation signal for 'this domain runs matching': it has
    at least one active bus_rules.matching_thresholds row. Mirrors
    dbt_project/macros/active_domain_sources.sql's convention -- a domain is
    onboarded into matching purely by adding rows to matching_thresholds/
    matching_rules, never by editing this script."""
    return [
        r[0] for r in con.execute(
            "SELECT DISTINCT domain_key FROM bus_rules.matching_thresholds WHERE active"
        ).fetchall()
    ]


def _load_matching_metadata(con, domain_key):
    """Reads the two matching tables (bus_rules schema, DB-native, maintained
    via the Rules Configuration screen's maker-checker workflow), scoped to
    one domain, and returns (tiers, rules_by_tier), where tiers is the list
    of that domain's active tier rows (as dicts) from matching_thresholds,
    and rules_by_tier maps tier_id -> list of that tier's active rule rows
    from matching_rules, in rule_order."""
    thresholds = con.execute("""
        SELECT * FROM bus_rules.matching_thresholds
        WHERE active AND domain_key = ? ORDER BY tier_order
    """, [domain_key]).fetchdf().to_dict(orient="records")

    rules = con.execute("""
        SELECT * FROM bus_rules.matching_rules
        WHERE active AND domain_key = ? ORDER BY tier_id, rule_order
    """, [domain_key]).fetchdf().to_dict(orient="records")

    rules_by_tier = {}
    for r in rules:
        rules_by_tier.setdefault(r["tier_id"], []).append(r)

    return thresholds, rules_by_tier


def _run_domain_matching(con, domain_key, now):
    """Runs the full tier-1/tier-2 matching algorithm for one domain and
    returns (match_group_rows, edge_rows, review_candidate_rows) -- each
    already tagged with domain_key, ready to insert into the shared
    gold_prep tables. Identical algorithm to pre-Phase-3, just parameterized
    on domain_key instead of assuming 'customer'."""
    thresholds, rules_by_tier = _load_matching_metadata(con, domain_key)
    match_tiers = [t for t in thresholds if t["is_match_tier"]]

    # Phase 0 (flexibility hardening): this script only knows how to execute
    # two match_method values (below). Before this check existed, an active
    # tier with any other match_method -- or a *second* active tier reusing
    # 'exact'/'fuzzy_tfidf_cosine' -- was silently dropped from matching
    # entirely (next(...) only ever finds the first match), so a steward
    # could approve a new matching tier through the Rules Configuration UI
    # and it would just do nothing, with no error anywhere. Fail loudly
    # instead until a real handler-registry for match_method exists
    # (mirroring TRANSFORM_FUNCTIONS above).
    KNOWN_MATCH_METHODS = {"exact", "fuzzy_tfidf_cosine"}
    unknown_methods = {t["match_method"] for t in match_tiers} - KNOWN_MATCH_METHODS
    if unknown_methods:
        raise RuntimeError(
            f"bus_rules.matching_thresholds has active tier(s) for domain '{domain_key}' with match_method "
            f"{sorted(unknown_methods)} -- no matching logic is implemented for "
            f"{'that' if len(unknown_methods) == 1 else 'those'} yet, so "
            f"{'it' if len(unknown_methods) == 1 else 'they'} would silently be "
            f"excluded from matching. Known methods: {sorted(KNOWN_MATCH_METHODS)}. "
            f"Add a handler in scripts/generate_matches.py before using this match_method."
        )
    exact_tiers = [t for t in match_tiers if t["match_method"] == "exact"]
    fuzzy_tiers = [t for t in match_tiers if t["match_method"] == "fuzzy_tfidf_cosine"]
    if len(exact_tiers) > 1 or len(fuzzy_tiers) > 1:
        raise RuntimeError(
            f"Only one active tier per match_method is currently supported "
            f"('exact', 'fuzzy_tfidf_cosine') -- domain '{domain_key}' has multiple active tiers "
            f"sharing the same match_method in bus_rules.matching_thresholds, "
            f"and only the first would actually be used, silently ignoring the "
            f"rest. Deactivate the extras, or extend this script to support "
            f"multiple tiers per method."
        )
    tier1 = exact_tiers[0] if exact_tiers else None
    tier2 = fuzzy_tiers[0] if fuzzy_tiers else None
    if tier1 is None:
        raise RuntimeError(
            f"No active tier with match_method='exact' found in bus_rules.matching_thresholds "
            f"for domain '{domain_key}' -- at least one exact tier is required."
        )

    exact_rules = [r for r in rules_by_tier.get(tier1["tier_id"], []) if r["rule_role"] == "exact_match_field"]
    tier1_confidence = float(tier1["auto_merge_threshold"])

    silver_df = con.execute(
        "SELECT source_system, source_record_id, attributes FROM main_silver.records WHERE domain_key = ?",
        [domain_key],
    ).fetchdf()
    records = []
    for _, srow in silver_df.iterrows():
        attrs = json.loads(srow["attributes"]) if srow["attributes"] else {}
        rec = dict(attrs)
        rec["source_system"] = srow["source_system"]
        rec["source_record_id"] = srow["source_record_id"]
        records.append(rec)
    keys = [(r["source_system"], r["source_record_id"]) for r in records]
    by_key = dict(zip(keys, records))

    overrides = con.execute(
        "SELECT pair_id, status FROM stewardship.match_review_overrides"
    ).fetchdf()
    override_status = dict(zip(overrides["pair_id"], overrides["status"]))

    # --- Match Review hold: figure out which silver records sit out of this
    # run entirely (see module docstring). 'confirmed' is deliberately
    # excluded from the hold set -- this run needs to still process it, to
    # actually realize the merge. Identity for a held pair (which two source
    # records it's between) comes from THIS DOMAIN's slice of
    # gold_prep.match_review_candidates as it existed BEFORE this run
    # rebuilds it -- that table is this script's own last-written output, so
    # it's authoritative regardless of whether a `dbt run --select gold.*`
    # has happened since.
    OPEN_HOLD_STATUSES = {"pending", "in_review"}
    on_hold_pair_ids = {pid for pid, status in override_status.items() if status in OPEN_HOLD_STATUSES}

    prior_candidates = con.execute(
        "SELECT * FROM gold_prep.match_review_candidates WHERE domain_key = ?", [domain_key]
    ).fetchdf().to_dict(orient="records")

    on_hold_keys = set()
    carried_forward_candidates = []
    for row in prior_candidates:
        if row["pair_id"] not in on_hold_pair_ids:
            continue
        carried_forward_candidates.append(row)
        on_hold_keys.add((row["source_system_a"], row["source_record_id_a"]))
        on_hold_keys.add((row["source_system_b"], row["source_record_id_b"]))

    # --- Tier 1: exact edges, one attribute-value grouping per active exact_match_field rule ---
    exact_edges = []  # (key_a, key_b, confidence)
    for rule in exact_rules:
        groups = {}
        for k, r in by_key.items():
            if k in on_hold_keys:
                continue  # sits out of this run entirely, see Match Review hold docstring
            val = _apply_transform(rule["transform_function"], r.get(rule["attribute_key"]))
            if val:
                groups.setdefault(val, []).append(k)
        for group in groups.values():
            for i in range(len(group)):
                for j in range(i + 1, len(group)):
                    exact_edges.append((group[i], group[j], tier1_confidence))

    # --- Tier 2: fuzzy candidate generation via blocking + TF-IDF cosine similarity ---
    fuzzy_auto_edges = []       # (key_a, key_b, confidence) -- similarity >= auto_merge_threshold
    fuzzy_confirmed_edges = []  # steward-confirmed borderline pairs
    review_candidates = []      # rows for gold_prep.match_review_candidates

    if tier2 is not None:
        similarity_rules = [r for r in rules_by_tier.get(tier2["tier_id"], []) if r["rule_role"] == "similarity_text_field"]
        blocking_rules = [r for r in rules_by_tier.get(tier2["tier_id"], []) if r["rule_role"] == "blocking_key"]
        tau_high = float(tier2["auto_merge_threshold"])
        tau_low = float(tier2["review_lower_threshold"])

        blocks = {}
        for k, r in by_key.items():
            if k in on_hold_keys:
                continue  # sits out of this run entirely, see Match Review hold docstring
            blocks.setdefault(_blocking_key(r, blocking_rules), []).append(k)

        texts = [_combined_text(r, similarity_rules) for r in records]
        vectorizer = TfidfVectorizer(analyzer=TFIDF_ANALYZER, ngram_range=TFIDF_NGRAM_RANGE)
        if texts:
            X = vectorizer.fit_transform(texts)
            key_to_row = {k: i for i, k in enumerate(keys)}

            already_exact = set()
            for a, b, _ in exact_edges:
                already_exact.add(frozenset((a, b)))

            for block_val, block_keys in blocks.items():
                for i in range(len(block_keys)):
                    for j in range(i + 1, len(block_keys)):
                        ka, kb = block_keys[i], block_keys[j]
                        if ka[0] == kb[0]:
                            continue  # same source system can't be a cross-source duplicate pair
                        if frozenset((ka, kb)) in already_exact:
                            continue  # already an exact match, no need for a fuzzy edge too
                        sim = float(cosine_similarity(X[key_to_row[ka]], X[key_to_row[kb]])[0, 0])
                        if sim < tau_low:
                            continue
                        if sim >= tau_high:
                            fuzzy_auto_edges.append((ka, kb, sim))
                            continue
                        # borderline: honor any prior steward decision, else queue for review
                        pid = _pair_id(ka, kb)
                        status = override_status.get(pid)
                        if status == "confirmed":
                            fuzzy_confirmed_edges.append((ka, kb, sim))
                        elif status == "rejected":
                            pass  # permanently excluded, don't resurface
                        review_candidates.append({
                            "pair_id": pid,
                            "source_system_a": ka[0], "source_record_id_a": ka[1],
                            "source_system_b": kb[0], "source_record_id_b": kb[1],
                            "similarity_score": round(sim, 4),
                            "blocking_key": str(block_val),
                        })

    # --- Clustering ---
    uf = UnionFind(keys)
    all_auto_edges = exact_edges + fuzzy_auto_edges + fuzzy_confirmed_edges
    for a, b, _ in all_auto_edges:
        uf.union(a, b)

    clusters = {}
    for k in keys:
        clusters.setdefault(uf.find(k), []).append(k)
    ordered_roots = sorted(clusters.keys(), key=lambda root: min(clusters[root]))
    match_group_id = {}
    for gid, root in enumerate(ordered_roots, start=1):
        for k in clusters[root]:
            match_group_id[k] = gid

    group_rows = [(domain_key, k[0], k[1], match_group_id[k]) for k in keys]
    edge_rows = (
        [(domain_key, a[0], a[1], b[0], b[1], "exact", conf, now) for a, b, conf in exact_edges]
        + [(domain_key, a[0], a[1], b[0], b[1], "fuzzy_auto", conf, now) for a, b, conf in fuzzy_auto_edges]
        + [(domain_key, a[0], a[1], b[0], b[1], "fuzzy_confirmed", conf, now) for a, b, conf in fuzzy_confirmed_edges]
    )
    # Held pairs' rows are carried forward frozen (their own original
    # generated_ts, not this run's `now`) alongside freshly computed ones --
    # see Match Review hold in the module docstring.
    all_review_candidates = review_candidates + carried_forward_candidates
    review_rows = [
        (domain_key, rc["pair_id"], rc["source_system_a"], rc["source_record_id_a"],
         rc["source_system_b"], rc["source_record_id_b"], rc["similarity_score"],
         rc["blocking_key"], rc.get("generated_ts", now))
        for rc in all_review_candidates
    ]

    tau_high_display = tier2["auto_merge_threshold"] if tier2 is not None else "n/a"
    tau_low_display = tier2["review_lower_threshold"] if tier2 is not None else "n/a"
    print(f"  [{domain_key}] {len(keys)} silver records -> {len(ordered_roots)} match groups")
    print(f"    on-hold (excluded from this run, open match review): {len(on_hold_keys)} silver records")
    print(f"    tier-1 exact edges: {len(exact_edges)}")
    print(f"    tier-2 auto-merge edges (>= {tau_high_display}): {len(fuzzy_auto_edges)}")
    print(f"    tier-2 steward-confirmed edges: {len(fuzzy_confirmed_edges)}")
    print(f"    match review candidates ({tau_low_display}-{tau_high_display}, this run): {len(review_candidates)}"
          f" + {len(carried_forward_candidates)} carried forward (held)")

    return group_rows, edge_rows, review_rows


def main():
    con = duckdb.connect(DB_PATH)
    _ensure_match_review_tables(con)
    _ensure_gold_prep_tables(con)

    domains = _domains_with_matching_config(con)
    if not domains:
        print("No domain has any active bus_rules.matching_thresholds row -- nothing to match.")
        con.close()
        return

    now = datetime.now(timezone.utc)
    print(f"Matching {len(domains)} domain(s): {domains}")

    for domain_key in domains:
        group_rows, edge_rows, review_rows = _run_domain_matching(con, domain_key, now)

        # gold_prep.* are shared across every domain (like bronze.raw_records)
        # -- each domain's run replaces only its own domain_key's rows.
        con.execute("DELETE FROM gold_prep.match_groups WHERE domain_key = ?", [domain_key])
        if group_rows:
            con.executemany("INSERT INTO gold_prep.match_groups VALUES (?, ?, ?, ?)", group_rows)

        con.execute("DELETE FROM gold_prep.match_edges WHERE domain_key = ?", [domain_key])
        if edge_rows:
            con.executemany("INSERT INTO gold_prep.match_edges VALUES (?, ?, ?, ?, ?, ?, ?, ?)", edge_rows)

        con.execute("DELETE FROM gold_prep.match_review_candidates WHERE domain_key = ?", [domain_key])
        if review_rows:
            con.executemany(
                "INSERT INTO gold_prep.match_review_candidates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", review_rows
            )

    con.close()


if __name__ == "__main__":
    main()
