-- Phase 3 (domain-agnostic) replacement for gold_customers.sql. Attribute-level
-- survivorship (Data Governance > Rules Configuration > Survivorship Rules):
-- each attribute below is independently picked by gold_survivorship_winners.sql,
-- per that attribute's own active bus_rules.survivorship_rules row (most_common /
-- most_complete / oldest / newest / pattern_match, tie-broken by
-- source_modified_date desc then governance.domain_source_systems.sort_order
-- -- see that model's docstring for the full rule). See gold_crosswalk.sql
-- for the per-attribute "which source won which attribute" detail this
-- produces.
--
-- golden_id is domain-qualified ('GOLD-<DOMAIN>-00001') rather than the
-- pre-Phase-3 'GOLD-00001': match_group_id (from scripts/generate_matches.py)
-- is only assigned uniquely WITHIN one domain's matching run, so a second
-- live domain would otherwise produce colliding IDs (customer match_group_id
-- 1 and product match_group_id 1 both rendering as GOLD-00001). This is a
-- deliberate, necessary format change flagged for Phase 4 (API) and Phase 5
-- (frontend), which must be updated to parse/generate this same format.
{% set domains = active_gold_domains() %}

with winners as (
    select * from {{ ref('gold_survivorship_winners') }}
),

{% set pivot_branches = [] %}
{% for domain_key in domains %}
  {% set attrs = domain_attribute_keys(domain_key) %}
  {% set pairs = [] %}
  {% for attr in attrs %}
    {% do pairs.append("'" ~ attr ~ "', max(case when target_column = '" ~ attr ~ "' then winning_value end)") %}
  {% endfor %}
  {% set branch %}
    select
        domain_key,
        match_group_id,
        json_object({{ pairs | join(', ') }}) as attributes
    from winners
    where domain_key = '{{ domain_key }}'
    group by domain_key, match_group_id
  {% endset %}
  {% do pivot_branches.append(branch) %}
{% endfor %}

pivoted as (
    {{ pivot_branches | join('\n    union all\n') }}
),

group_stats as (
    select domain_key, match_group_id, count(distinct source_system) as source_system_count
    from {{ ref('gold_match_candidates') }}
    group by domain_key, match_group_id
),

-- "Primary" survivor_source_system/survivor_source_record_id is kept for
-- backward compatibility (existing API/UI fields expect a single survivor
-- per golden record) -- defined as whichever contributing source won the
-- most individual attributes, ties broken the same way the per-attribute
-- rule itself is tie-broken (most recent source_modified_date, then
-- governance.domain_source_systems.sort_order -- see source_priority below,
-- the same tie-break gold_survivorship_winners.sql and api/reprocessing.py's
-- _source_priority() use, replacing the old hardcoded "CRM preferred").
column_win_counts as (
    select domain_key, match_group_id, source_system, source_record_id, count(*) as columns_won
    from winners
    group by domain_key, match_group_id, source_system, source_record_id
),

source_priority as (
    select domain_key, source_system_key as source_system, sort_order
    from {{ source('governance', 'domain_source_systems') }}
    where active
),

primary_survivor as (
    select
        c.domain_key, c.match_group_id, c.source_system, c.source_record_id,
        row_number() over (
            partition by c.domain_key, c.match_group_id
            order by
                c.columns_won desc,
                coalesce(epoch(try_cast(cand.attributes->>'source_modified_date' as timestamp)), -1) desc,
                coalesce(sp.sort_order, 999999)
        ) as rn
    from column_win_counts c
    join {{ ref('gold_match_candidates') }} cand
      on cand.domain_key = c.domain_key
     and cand.match_group_id = c.match_group_id
     and cand.source_system = c.source_system
     and cand.source_record_id = c.source_record_id
    left join source_priority sp
      on sp.domain_key = c.domain_key and sp.source_system = c.source_system
)

select
    'GOLD-' || upper(p.domain_key) || '-' || lpad(cast(p.match_group_id as varchar), 5, '0') as golden_id,
    p.domain_key,
    p.match_group_id,
    p.attributes,
    gs.source_system_count,
    ps.source_system as survivor_source_system,
    ps.source_record_id as survivor_source_record_id,
    current_timestamp as gold_curated_ts
from pivoted p
join group_stats gs on gs.domain_key = p.domain_key and gs.match_group_id = p.match_group_id
join primary_survivor ps on ps.domain_key = p.domain_key and ps.match_group_id = p.match_group_id and ps.rn = 1
