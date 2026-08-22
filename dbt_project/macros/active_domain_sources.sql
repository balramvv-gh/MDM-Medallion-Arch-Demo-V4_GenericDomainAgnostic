{#
  Phase 2 (domain-agnostic): the compile-time-discovered list of
  (domain_key, source_system_key) combos the generic silver staging model
  (models/silver/stg_source_records.sql) should build a UNION branch for.
  Driven entirely by which rows exist in governance.domain_source_systems --
  a domain with no source_system rows (e.g. the 'product' placeholder, which
  has no pipeline yet) is automatically skipped, no pipeline_status check or
  domain-name test required. Onboarding a new live domain/source is exactly
  "add rows here (and to domain_attributes/source_field_mappings)", never
  "add a branch to this file".

  Returns a Jinja list of (domain_key, source_system_key) tuples, ordered by
  domain sort_order then source sort_order.
#}
{% macro active_domain_sources() %}
  {%- set combos = [] -%}
  {%- if execute -%}
    {%- set query -%}
      select ds.domain_key, ds.source_system_key
      from {{ source('governance', 'domain_source_systems') }} ds
      join {{ source('governance', 'domain_registry') }} d on d.domain_key = ds.domain_key
      where ds.active and d.is_active
      order by d.sort_order, ds.sort_order
    {%- endset -%}
    {%- set results = run_query(query) -%}
    {%- for row in results.rows -%}
      {%- do combos.append((row[0], row[1])) -%}
    {%- endfor -%}
  {%- endif -%}
  {{ return(combos) }}
{% endmacro %}

{#
  Phase 3 (domain-agnostic): the compile-time-discovered list of domain_keys
  the gold layer (models/gold/*.sql) should build a branch for. Driven
  entirely by which domain_keys scripts/generate_matches.py actually wrote
  rows for in gold_prep.match_groups -- a domain with no matching_thresholds
  configured (e.g. the 'product' placeholder) never gets a gold_prep row, so
  it's automatically skipped here too, no pipeline_status check needed.
#}
{% macro active_gold_domains() %}
  {%- set domains = [] -%}
  {%- if execute -%}
    {%- set relation = adapter.get_relation(database=target.database, schema='gold_prep', identifier='match_groups') -%}
    {%- if relation is not none -%}
      {%- set query -%}
        select distinct domain_key from {{ source('gold_prep', 'match_groups') }} order by domain_key
      {%- endset -%}
      {%- set results = run_query(query) -%}
      {%- for row in results.rows -%}
        {%- do domains.append(row[0]) -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endif -%}
  {{ return(domains) }}
{% endmacro %}
