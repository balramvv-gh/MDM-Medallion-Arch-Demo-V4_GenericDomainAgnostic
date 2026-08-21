-- Phase 3 (domain-agnostic): borderline fuzzy-match candidates
-- (TAU_LOW <= similarity < TAU_HIGH, see scripts/generate_matches.py) that
-- were NOT auto-merged into gold. Presented here for a data steward to
-- confirm ("yes, same record -- merge them") or reject ("no, coincidence --
-- keep them separate"). Attribute-bag shape (domain_key + attributes JSON
-- per side) instead of named Customer columns per side -- a second live
-- domain works here unchanged.
--
-- Mirrors exceptions_queue.sql's pattern deliberately: this table is a full
-- point-in-time snapshot of every borderline pair this pipeline run found.
-- LIVE review status lives in stewardship.match_review_overrides (joined at
-- query time by the API, same as exception_status_overrides), not here --
-- so a pair a steward already decided on keeps showing its history even
-- after this table gets rebuilt by the next `dbt run`.
with candidates as (
    select * from {{ source('gold_prep', 'match_review_candidates') }}
),

side_a as (
    select * from {{ ref('records') }}
),

side_b as (
    select * from {{ ref('records') }}
)

select
    c.domain_key,
    c.pair_id,
    c.similarity_score,
    c.blocking_key,
    a.source_system as source_system_a,
    a.source_record_id as source_record_id_a,
    a.attributes as attributes_a,
    b.source_system as source_system_b,
    b.source_record_id as source_record_id_b,
    b.attributes as attributes_b,
    'pending' as review_status,
    c.generated_ts as queued_ts
from candidates c
join side_a a on a.domain_key = c.domain_key and a.source_system = c.source_system_a and a.source_record_id = c.source_record_id_a
join side_b b on b.domain_key = c.domain_key and b.source_system = c.source_system_b and b.source_record_id = c.source_record_id_b
