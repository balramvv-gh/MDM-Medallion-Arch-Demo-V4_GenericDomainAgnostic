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
with silver as (
    select * from {{ ref('records') }}
),

groups as (
    select * from {{ source('gold_prep', 'match_groups') }}
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
