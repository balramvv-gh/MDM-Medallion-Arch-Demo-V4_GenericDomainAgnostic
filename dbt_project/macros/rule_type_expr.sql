{#
  Phase 2 (flexibility) handler registry for bus_rules.column_rules.rule_type,
  used by column_rule_flags() (see that macro for the caller). Mirrors
  TRANSFORM_FUNCTIONS in scripts/generate_matches.py and RULE_TYPE_HANDLERS in
  api/validation.py: a genuinely new reject rule_type means adding one
  {% elif %} branch here, not editing stg_crm_customers.sql/
  stg_erp_customers.sql or any other model.

  Returns a SQL boolean expression (as a string) that evaluates to true when
  `column` VIOLATES the rule, or none if `rule_type` isn't recognized --
  column_rule_flags() turns a none return into a hard compiler error so an
  unrecognized rule_type fails the dbt run loudly (Phase 0 principle) instead
  of silently producing no check.

  `column` must already be a safe, non-user-controlled SQL identifier (it's
  always one of this project's own staged column names, never DB data).
  `rule_param` IS DB data (a steward-edited value from bus_rules.column_rules)
  and is single-quote-escaped before being embedded as a SQL string literal.
#}
{% macro rule_type_expr(rule_type, column, rule_param) %}
  {%- if rule_param is not none -%}
    {%- set safe_param = rule_param | replace("'", "''") -%}
  {%- else -%}
    {%- set safe_param = none -%}
  {%- endif -%}

  {%- if rule_type == 'not_null' -%}
    {{ return(column ~ " is null") }}
  {%- elif rule_type == 'regex' -%}
    {{ return("(" ~ column ~ " is null or not regexp_matches(" ~ column ~ ", '" ~ safe_param ~ "'))") }}
  {%- elif rule_type == 'reference_state' -%}
    {{ return("(" ~ column ~ " is null or " ~ column ~ " not in (select state_code from " ~ source('ref', 'ref_state_codes') ~ " where is_active))") }}
  {%- elif rule_type == 'reference_country' -%}
    {{ return("(" ~ column ~ " is null or " ~ column ~ " not in (select country_code from " ~ source('ref', 'ref_country_codes') ~ " where is_active))") }}
  {%- else -%}
    {{ return(none) }}
  {%- endif -%}
{% endmacro %}
