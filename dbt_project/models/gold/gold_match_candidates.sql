-- Match strategy: hybrid deterministic + embedding-similarity, computed by
-- scripts/generate_matches.py (Phase 3: runs once per domain -- see that
-- script's docstring for the full algorithm and README.md for the build
-- sequence). This model just attaches the match_group_id that step computed
-- to each silver record, across every domain gold_prep has data for; it
-- stays its own model (rather than inlining into gold_customers) so
-- lineage/impact analysis can point at "the matching step" as a distinct
-- node, same as before. Attribute-bag shape (domain_key + attributes JSON)
-- means this join needs no per-domain field list -- a second live domain
-- works here with zero changes.
--
-- Suspect-match gold hold: a silver record is withheld from gold entirely
-- (no golden record, no crosswalk row -- simply absent from this model) if
-- BOTH of these hold:
--   1. its match_group_id is a true singleton in gold_prep.match_groups
--      (group_size = 1) -- i.e. generate_matches.py did not auto-merge it
--      with anything via an exact or auto-merge-threshold fuzzy edge; and
--   2. it is a party to a Match Review pair (gold_prep.match_review_candidates)
--      that has no decision yet, or a decision still working through the
--      confirm/reject maker-checker workflow -- no row at all in
--      stewardship.match_review_overrides, or one with status 'pending' /
--      'in_review'.
-- Only singletons and confident (auto-merged) matches flow through to a
-- golden record on this path -- a suspect match's sides sit out of gold
-- until the steward's decision is confirmed and approved, per the Match
-- Review workflow. This never touches a record that's already part of a
-- multi-member group (group_size > 1): that merge was already established
-- by an unrelated edge and stays intact even if one of its members also
-- happens to be entangled in a separate, still-open suspect pairing with a
-- third record elsewhere.
--
-- A 'confirmed' pair's sides are deliberately NOT held here -- the next
-- full rebuild after confirmation re-clusters them together via
-- generate_matches.py's fuzzy_confirmed_edges union, same as any other
-- match; in between rebuilds, api/reprocessing.py's
-- reprocess_confirmed_match_review realizes the merge in real time. A
-- 'rejected' pair's sides are likewise not held -- they were never unioned
-- by generate_matches.py in the first place, so they're ordinary singletons
-- here and get their own golden records the normal way (in real time via
-- api/reprocessing.py's reprocess_rejected_match_review, or the next
-- rebuild as a fallback).
with silver as (
    select * from {{ ref('records') }}
),

groups as (
    select * from {{ source('gold_prep', 'match_groups') }}
),

group_sizes as (
    select domain_key, match_group_id, count(*) as group_size
    from groups
    group by domain_key, match_group_id
),

open_review_sides as (
    select domain_key, source_system_a as source_system, source_record_id_a as source_record_id
    from {{ source('gold_prep', 'match_review_candidates') }} c
    left join {{ source('stewardship', 'match_review_overrides') }} o on o.pair_id = c.pair_id
    where coalesce(o.status, 'pending') in ('pending', 'in_review')

    union

    select domain_key, source_system_b as source_system, source_record_id_b as source_record_id
    from {{ source('gold_prep', 'match_review_candidates') }} c
    left join {{ source('stewardship', 'match_review_overrides') }} o on o.pair_id = c.pair_id
    where coalesce(o.status, 'pending') in ('pending', 'in_review')
)

select
    s.domain_key,
    s.source_system,
    s.source_record_id,
    s.attributes,
    g.match_group_id
from silver s
join groups g
  on g.domain_key = s.domain_key
 and g.source_system = s.source_system
 and g.source_record_id = s.source_record_id
join group_sizes gs
  on gs.domain_key = g.domain_key
 and gs.match_group_id = g.match_group_id
left join open_review_sides orp
  on orp.domain_key = s.domain_key
 and orp.source_system = s.source_system
 and orp.source_record_id = s.source_record_id
where not (gs.group_size = 1 and orp.source_system is not null)
