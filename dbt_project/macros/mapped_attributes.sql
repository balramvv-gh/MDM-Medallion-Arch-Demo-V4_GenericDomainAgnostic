{#
  Phase 2 (domain-agnostic): generates one standardized column per active
  governance.domain_attributes row for a given (domain_key, source_system),
  read at compile time via run_query -- same "query a governed table at
  compile time" pattern column_rule_flags.sql already established. This is
  what replaces stg_crm_customers.sql / stg_erp_customers.sql's hand-written,
  per-source standardization -- a NEW domain or source is onboarded entirely
  by adding domain_attributes/source_field_mappings rows, never by adding or
  editing a dbt model.

  For each active domain_attributes row (ordered by sort_order):
    - If an active governance.source_field_mappings row exists for
      (domain_key, source_system, attribute_key), apply
      transform_expr(transform_function, source_expr) and, when the
      attribute's data_type is 'date', wrap the result in try_cast(... as
      date) (domain_attributes.data_type drives the cast -- no per-attribute
      hardcoding here).
    - If no mapping row exists (e.g. ERP has no source_created_date field),
      emit a typed NULL instead -- exactly matching pre-Phase-2
      stg_erp_customers.sql's `cast(null as date) as source_created_date`,
      just generalized to any domain/attribute/source combination instead of
      one hardcoded case.

  Returns a comma-separated (no trailing comma) block of
  `<expr> as <attribute_key>` column definitions, meant to be spliced into a
  `select` list. Companion macro domain_attribute_keys() returns the same
  ordered attribute_key list as a plain Jinja list (not SQL), for callers
  that need to reference these columns again afterward (e.g. to repack them
  into a JSON `attributes` bag -- see pack_attributes_json()).
#}
{% macro domain_attribute_keys(domain_key) %}
  {%- set keys = [] -%}
  {%- if execute -%}
    {%- set query -%}
      select attribute_key
      from {{ source('governance', 'domain_attributes') }}
      where active and domain_key = '{{ domain_key }}'
      order by sort_order
    {%- endset -%}
    {%- set results = run_query(query) -%}
    {%- for row in results.rows -%}
      {%- do keys.append(row[0]) -%}
    {%- endfor -%}
  {%- endif -%}
  {{ return(keys) }}
{% endmacro %}

{% macro mapped_attributes(domain_key, source_system) %}
  {%- set parts = [] -%}
  {%- if execute -%}
    {%- set attrs_query -%}
      select attribute_key, data_type
      from {{ source('governance', 'domain_attributes') }}
      where active and domain_key = '{{ domain_key }}'
      order by sort_order
    {%- endset -%}
    {%- set attrs = run_query(attrs_query) -%}

    {%- set map_query -%}
      select attribute_key, source_expr, transform_function
      from {{ source('governance', 'source_field_mappings') }}
      where active and domain_key = '{{ domain_key }}' and source_system_key = '{{ source_system }}'
    {%- endset -%}
    {%- set mappings = run_query(map_query) -%}
    {%- set mapping_by_attr = {} -%}
    {%- for row in mappings.rows -%}
      {%- do mapping_by_attr.update({row[0]: (row[1], row[2])}) -%}
    {%- endfor -%}

    {%- for attr in attrs.rows -%}
      {%- set attribute_key, data_type = attr[0], attr[1] -%}
      {%- set sql_type = 'date' if data_type == 'date' else 'varchar' -%}
      {%- if attribute_key in mapping_by_attr -%}
        {%- set source_expr, transform_function = mapping_by_attr[attribute_key] -%}
        {%- set expr = transform_expr(transform_function, source_expr) -%}
        {%- if expr is none -%}
          {{ exceptions.raise_compiler_error(
              "mapped_attributes(" ~ domain_key ~ "." ~ source_system ~ "): unrecognized transform_function '" ~
              transform_function ~ "' for attribute_key '" ~ attribute_key ~ "' -- no branch in " ~
              "macros/transform_expr.sql. Add one there before this transform_function can be used."
          ) }}
        {%- endif -%}
        {%- if data_type == 'date' -%}
          {%- set expr = "try_cast(" ~ expr ~ " as date)" -%}
        {%- endif -%}
        {%- do parts.append(expr ~ " as " ~ attribute_key) -%}
      {%- else -%}
        {#- No mapping row for this attribute+source: this source simply
            doesn't supply it (e.g. ERP has no source_created_date). Typed
            NULL, not an error -- see this macro's docstring. -#}
        {%- do parts.append("cast(null as " ~ sql_type ~ ") as " ~ attribute_key) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {{ return(parts | join(',\n    ')) }}
{% endmacro %}

{#
  Repacks the attribute columns mapped_attributes() just produced (which
  MUST already be in scope in the same select, e.g. a CTE built from
  mapped_attributes()'s output) into one JSON `attributes` bag -- the
  attribute-bag shape silver.records/exceptions_queue store, matching
  bronze's own attributes-JSON shape. Takes the same domain_key
  domain_attribute_keys() would resolve, so it always packs exactly the
  columns mapped_attributes() generated for that domain, in the same order.
#}
{% macro pack_attributes_json(domain_key) %}
  {%- set keys = domain_attribute_keys(domain_key) -%}
  {%- set pairs = [] -%}
  {%- for k in keys -%}
    {%- do pairs.append("'" ~ k ~ "', " ~ k) -%}
  {%- endfor -%}
  {{ return("json_object(" ~ (pairs | join(', ')) ~ ")") }}
{% endmacro %}
