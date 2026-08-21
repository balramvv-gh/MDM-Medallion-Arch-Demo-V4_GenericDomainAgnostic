-- Phase 3 (domain-agnostic): this table is the answer to "how do we keep
-- gold linked back to source?" -- every source record that contributed to a
-- golden record has a row here, with a match confidence score and, since
-- attribute-level survivorship (Data Governance > Rules Configuration >
-- Survivorship Rules), the specific list of gold attributes that record's
-- value won (winning_attributes) -- gold_survivorship_winners.sql picks the
-- winner independently per attribute, so a single golden record can
-- legitimately draw its email from one source and its address from another.
-- is_survivor_record is kept for backward compatibility (existing UI's
-- "★ survivor" badge) and is now simply "won at least one attribute" (true
-- whenever winning_attributes is non-empty) -- a record-level flag derived
-- from the attribute-level truth, not the other way around.
--
-- Confidence score is genuinely graduated (see scripts/generate_matches.py),
-- and every value below is read from bus_rules.matching_thresholds (DB-native,
-- maintained via the Rules Configuration screen's maker-checker workflow,
-- now domain_key-scoped since scripts/migrate_matching_rules_to_domain.py)
-- rather than hardcoded, so the crosswalk can't drift from the same metadata
-- the batch matching step and the real-time reprocessing path
-- (api/reprocessing.py) use:
--   tier 1's auto_merge_threshold  -- connected to another group member via an
--                                      exact-match tier (e.g. email/phone)
--   <similarity>                   -- connected via a fuzzy tier, auto-merged or
--                                      steward-confirmed -- the actual cosine
--                                      similarity score, typically 0.80-1.00
--   no_match_baseline's value       -- single-source record, no corroborating
--                                      match at all ("provisional")
with candidates as (
    select * from {{ ref('gold_match_candidates') }}
),

group_stats as (
    select
        domain_key,
        match_group_id,
        count(distinct source_system) as source_system_count
    from candidates
    group by domain_key, match_group_id
),

edges as (
    select * from {{ source('gold_prep', 'match_edges') }}
),

-- best (max) confidence for each source record from any edge touching it --
-- a record can appear as either side of an edge, so union both directions
edge_confidence as (
    select domain_key, source_system, source_record_id, max(confidence) as confidence
    from (
        select domain_key, source_system_a as source_system, source_record_id_a as source_record_id, confidence from edges
        union all
        select domain_key, source_system_b as source_system, source_record_id_b as source_record_id, confidence from edges
    )
    group by domain_key, source_system, source_record_id
),

-- Per-domain lookups from the matching_thresholds seed for the two fallback
-- confidence values (used only when a record has no edge at all touching it --
-- see the coalesce below).
tier1_fallback as (
    select domain_key, auto_merge_threshold as confidence
    from {{ source('bus_rules', 'matching_thresholds') }}
    where active and is_match_tier and match_method = 'exact'
    qualify row_number() over (partition by domain_key order by tier_order) = 1
),

baseline_fallback as (
    select domain_key, auto_merge_threshold as confidence
    from {{ source('bus_rules', 'matching_thresholds') }}
    where active and not is_match_tier and match_method = 'no_match_baseline'
    qualify row_number() over (partition by domain_key order by tier_order) = 1
),

-- Per-attribute survivorship winners (gold_survivorship_winners.sql), grouped
-- by contributing source record into the list of gold attributes that
-- source won -- e.g. a record might win ['email','phone'] while another
-- record in the same group wins ['address_line1','city'].
won_columns as (
    select
        domain_key, match_group_id, source_system, source_record_id,
        list(target_column order by target_column) as winning_attributes
    from {{ ref('gold_survivorship_winners') }}
    group by domain_key, match_group_id, source_system, source_record_id
)

select
    'GOLD-' || upper(c.domain_key) || '-' || lpad(cast(c.match_group_id as varchar), 5, '0') as golden_id,
    c.domain_key,
    c.source_system,
    c.source_record_id,
    -- A record with no edge at all touching it (ec.confidence null) is either
    -- a defensive fallback for a multi-source group that somehow has no
    -- recorded edge (shouldn't normally happen -- tier1_fallback), or a truly
    -- isolated single-source record (baseline_fallback, the "provisional" score).
    coalesce(
        ec.confidence,
        case when gs.source_system_count > 1
             then tf.confidence
             else bf.confidence
        end
    ) as match_confidence_score,
    coalesce(wc.winning_attributes, []) as winning_attributes,
    coalesce(len(wc.winning_attributes) > 0, false) as is_survivor_record,
    current_timestamp as crosswalk_created_ts
from candidates c
join group_stats gs on gs.domain_key = c.domain_key and gs.match_group_id = c.match_group_id
left join edge_confidence ec
  on ec.domain_key = c.domain_key and ec.source_system = c.source_system and ec.source_record_id = c.source_record_id
left join tier1_fallback tf on tf.domain_key = c.domain_key
left join baseline_fallback bf on bf.domain_key = c.domain_key
left join won_columns wc
  on wc.domain_key = c.domain_key
 and wc.match_group_id = c.match_group_id
 and wc.source_system = c.source_system
 and wc.source_record_id = c.source_record_id
