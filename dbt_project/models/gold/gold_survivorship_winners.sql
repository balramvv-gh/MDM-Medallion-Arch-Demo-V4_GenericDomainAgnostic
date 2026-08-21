-- Phase 3 (domain-agnostic): attribute-level survivorship (Data Governance >
-- Rules Configuration > Survivorship Rules), generalized from a fixed
-- 10-column Customer pivot to every active governance.domain_attributes
-- attribute, for every domain gold_prep has data for (macros/
-- active_domain_sources.sql's active_gold_domains()/domain_attribute_keys()).
-- For each (domain_key, match_group_id, attribute_key), this model picks the
-- single contributing source record whose value wins that attribute, per
-- bus_rules.survivorship_rules -- at most one active rule per
-- (domain_key, attribute_key), one of:
--   most_common    -- the non-blank value that appears in the most
--                     contributing records for that attribute
--   most_complete  -- prefers a non-blank value over a blank/null one
--   oldest/newest  -- by that source record's source_modified_date
--   pattern_match  -- prefers a value matching rule_param (a regex)
--   shortest_value -- prefers the shortest non-blank value (e.g. picking the
--                     cleaner of two address-line variants)
-- An attribute with NO configured survivorship_rules row (any domain's
-- source_created_date/source_modified_date today -- Phase 1 seeded them as
-- attributes, but Phase 3 hasn't seeded survivorship rules for them, same as
-- pre-Phase-2 behavior) falls back to the universal, non-configurable
-- tie-break instead of erroring: source_modified_date desc, then
-- governance.domain_source_systems.sort_order (lower wins) -- exactly one
-- primary rule per attribute plus one fixed tie-break, not an arbitrary
-- stack (see bus_rules.survivorship_rules' docstring in api/db.py for the
-- governance rationale). This must stay in lockstep with
-- api/reprocessing.py's _source_priority()/_pick_attribute_winners() --
-- the equivalent tie-break for the real-time reprocessing path -- so the
-- two paths can't silently disagree on which source wins a tie for a
-- future domain.
--
-- A null/unparseable source_modified_date is treated as the oldest possible
-- (epoch -1), the same convention api/reprocessing.py's _epoch() helper uses
-- for the identical situation on the real-time reprocessing path -- so a
-- missing date never wins 'newest' and never wins a tie-break, but *can*
-- still legitimately win an explicit 'oldest' rule if every other candidate
-- is also missing a date (a demo-scope edge case, consistent with how the
-- rest of this project treats missing dates).
--
-- Downstream: gold_customers.sql (Phase 3: renamed golden_records.sql)
-- pivots this back to one row per golden record's attributes JSON;
-- gold_crosswalk.sql groups this by source to report which attributes each
-- contributing source record actually won (winning_attributes).
{% set domains = active_gold_domains() %}

with candidates as (
    select * from {{ ref('gold_match_candidates') }}
),

rules as (
    select * from {{ source('bus_rules', 'survivorship_rules') }}
    where active
),

-- Tie-break preference order for "which source wins when otherwise equal"
-- (lower sort_order wins) -- replaces the old hardcoded "CRM preferred",
-- read from the same governance.domain_source_systems table
-- api/reprocessing.py's _source_priority() reads for the real-time path.
source_priority as (
    select domain_key, source_system_key as source_system, sort_order
    from {{ source('governance', 'domain_source_systems') }}
    where active
),

-- Unpivot every active attribute of every domain gold_prep has data for into
-- long format: one row per (domain_key, match_group_id, contributing source
-- record, attribute_key).
{% set long_branches = [] %}
{% for domain_key in domains %}
  {% set attrs = domain_attribute_keys(domain_key) %}
  {% for attr in attrs %}
    {% set branch %}
    select
        match_group_id,
        domain_key,
        source_system,
        source_record_id,
        try_cast(attributes->>'source_modified_date' as date) as source_modified_date,
        '{{ attr }}' as target_column,
        attributes->>'{{ attr }}' as value
    from candidates
    where domain_key = '{{ domain_key }}'
    {% endset %}
    {% do long_branches.append(branch) %}
  {% endfor %}
{% endfor %}

long as (
    {{ long_branches | join('\n    union all\n') }}
),

scored as (
    select
        l.*,
        coalesce(r.rule_type, 'newest') as rule_type,
        nullif(trim(l.value), '') as clean_value,
        coalesce(epoch(cast(l.source_modified_date as timestamp)), -1) as date_epoch,
        case coalesce(r.rule_type, 'newest')
            when 'newest' then coalesce(epoch(cast(l.source_modified_date as timestamp)), -1)
            when 'oldest' then -1 * coalesce(epoch(cast(l.source_modified_date as timestamp)), -1)
            when 'most_complete' then
                case when nullif(trim(l.value), '') is not null then 1 else 0 end
            when 'pattern_match' then
                case when nullif(trim(l.value), '') is not null
                          and r.rule_param is not null
                          and regexp_matches(l.value, r.rule_param)
                     then 1 else 0 end
            when 'most_common' then 0  -- scored in the freq CTE below instead
            when 'shortest_value' then
                -- A genuinely new rule_type is a single isolated `when`
                -- branch here -- no other file changes. Blank/null never
                -- wins: it scores far below any real value, the same intent
                -- as 'most_complete' above. Among non-blank values, a
                -- shorter length ranks higher (less negative).
                case when nullif(trim(l.value), '') is not null
                     then -1 * length(trim(l.value))
                     else -999999
                end
            -- Phase 0 (flexibility hardening): a rule_type with no branch above
            -- used to fall through to 0 here silently -- same score as an
            -- unmatched 'most_common' row, so a brand-new survivorship
            -- rule_type looked "saved" in the UI but produced no real
            -- scoring logic and nobody would notice. Fail the dbt run loudly
            -- instead: add a real `when` branch above before this rule_type
            -- can be used (see 'shortest_value' above for how that's done).
            else error(
                'Unrecognized survivorship rule_type ' || coalesce(r.rule_type, 'NULL') ||
                ' for domain ' || l.domain_key || ', attribute ' || l.target_column ||
                ' -- no scoring logic implemented in gold_survivorship_winners.sql. ' ||
                'Known rule_types: most_common, most_complete, oldest, newest, pattern_match, shortest_value.'
            )
        end as base_rank_key
    from long l
    left join rules r on r.domain_key = l.domain_key and r.attribute_key = l.target_column
),

-- 'most_common': frequency of each non-blank value within
-- (domain_key, match_group_id, target_column). Blank/null values never
-- count toward another value's frequency and can never themselves be "the
-- most common value" -- they fall through to the tie-break like any other
-- non-winner.
freq as (
    select domain_key, match_group_id, target_column, clean_value, count(*) as value_count
    from scored
    where clean_value is not null
    group by 1, 2, 3, 4
),

ranked as (
    select
        s.*,
        case when s.rule_type = 'most_common' then coalesce(f.value_count, 0)
             else s.base_rank_key
        end as rank_key
    from scored s
    left join freq f
      on f.domain_key = s.domain_key
     and f.match_group_id = s.match_group_id
     and f.target_column = s.target_column
     and f.clean_value = s.clean_value
),

winners as (
    select
        r.*,
        row_number() over (
            partition by r.domain_key, r.match_group_id, r.target_column
            order by
                r.rank_key desc,
                r.date_epoch desc,
                coalesce(sp.sort_order, 999999)
        ) as winner_rank
    from ranked r
    left join source_priority sp
      on sp.domain_key = r.domain_key and sp.source_system = r.source_system
)

select
    domain_key,
    match_group_id,
    target_column,
    source_system,
    source_record_id,
    source_modified_date,
    value as winning_value
from winners
where winner_rank = 1
