{#
  Phase 2 (domain-agnostic) handler registry for
  governance.source_field_mappings.transform_function, used by
  mapped_attributes() (see that macro for the caller). Mirrors
  rule_type_expr.sql's registry pattern exactly: a genuinely new
  standardization transform means adding one {% elif %} branch here, never
  editing a per-domain or per-source model file.

  Returns a SQL expression (as a string) that standardizes `expr` (already a
  SQL expression, e.g. attributes->>'first_name' or a split_part(...) call --
  never a bare column name assumption), or none if `transform_function` isn't
  recognized -- mapped_attributes() turns a none return into a hard compiler
  error so an unrecognized transform_function fails the dbt run loudly
  (same Phase 0 principle as rule_type_expr.sql) instead of silently passing
  data through unstandardized.
#}
{% macro transform_expr(transform_function, expr) %}
  {%- if transform_function == 'none' -%}
    {{ return(expr) }}
  {%- elif transform_function == 'trim' -%}
    {{ return("nullif(trim(" ~ expr ~ "), '')") }}
  {%- elif transform_function == 'trim_proper_case' -%}
    {{ return("nullif(trim(regexp_replace(" ~ proper_case(expr) ~ ", '\\s+', ' ')), '')") }}
  {%- elif transform_function == 'upper_trim' -%}
    {{ return("upper(nullif(trim(" ~ expr ~ "), ''))") }}
  {%- elif transform_function == 'lowercase_trim' -%}
    {{ return("lower(nullif(trim(" ~ expr ~ "), ''))") }}
  {%- elif transform_function == 'phone_e164_us' -%}
    {{ return(
        "case when regexp_replace(coalesce(" ~ expr ~ ", ''), '[^0-9]', '', 'g') != '' " ~
        "then '+1' || right(regexp_replace(" ~ expr ~ ", '[^0-9]', '', 'g'), 10) else null end"
    ) }}
  {%- else -%}
    {{ return(none) }}
  {%- endif -%}
{% endmacro %}
