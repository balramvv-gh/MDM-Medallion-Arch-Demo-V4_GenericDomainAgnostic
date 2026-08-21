{#
  Phase 2 (flexibility): generates reject-check boolean columns for a
  staging model directly from active bus_rules.column_rules rows, queried at
  *compile time* via run_query -- instead of each err_* column being a
  hand-written boolean expression that has to be kept in sync, by hand, in
  both stg_crm_customers.sql and stg_erp_customers.sql (e.g. today's email
  regex is the literal same pattern typed out in both files). A new rule row
  for an EXISTING rule_type on one of these fields now changes the batch
  pipeline's validation with zero SQL edits -- e.g. tightening the email
  regex via Rules Configuration takes effect on the next `dbt run`. Adding a
  genuinely new rule_type is a single addition to rule_type_expr() (see that
  macro), not a change here or in either staging model.

  Call once per source_system, passing the list of canonical fields that
  system's model wants machine-generated, e.g.:
      {{ column_rule_flags('CRM', ['first_name', 'last_name', 'email', 'state_code', 'country_code']) }}
  Returns a comma-separated (no trailing comma) block of
  `(<expr>) as err_<...>` column definitions, meant to be spliced into a
  `select` list.

  Scope, deliberately -- read before adding a field to `canonical_fields`:
    - Every requested field MUST resolve to at least one active,
      reject-severity bus_rules.column_rules row for this source_system
      (via CANONICAL_COLUMN_MAP below) -- if not, this raises a compiler
      error rather than silently emitting `false` for that field. A
      silently-always-false check is exactly the kind of "looks configured,
      does nothing" bug Phase 0 was written to prevent; better to fail the
      dbt run and say so.
    - Only request a field here if it maps onto a plain "does this staged
      column violate this rule" check. Two real fields in this project
      don't and are deliberately left hand-written in their model instead:
        - CRM has no bus_rules.column_rules row for phone at all (R001-R005
          cover first_name/last_name/email/state/country only) -- CRM's
          err_missing_phone stays hand-written as `(phone is null)`.
        - ERP's full_name (R009) isn't a plain not_null check on a staged
          column -- full_name gets split into first_name/last_name during
          standardization, so "is it missing" is a property of that split,
          not of a single canonical column. err_missing_first_name/
          err_missing_last_name stay hand-written in stg_erp_customers.sql.
      Requesting either of these here would raise a compiler error (no rule
      row maps to them), which is intentional -- it's a guardrail against
      silently regenerating them wrong later, not a bug to work around.
#}
{% macro column_rule_flags(source_system, canonical_fields) %}
  {%- set canonical_column_map = {
      'first_name': 'first_name', 'last_name': 'last_name',
      'email': 'email', 'email_addr': 'email',
      'phone': 'phone', 'contact_phone': 'phone',
      'state': 'state_code', 'state_code': 'state_code',
      'country': 'country_code', 'country_code': 'country_code'
  } -%}
  {%- set err_column_name = {
      'first_name': 'err_missing_first_name', 'last_name': 'err_missing_last_name',
      'email': 'err_invalid_email', 'phone': 'err_missing_phone',
      'state_code': 'err_invalid_state', 'country_code': 'err_invalid_country'
  } -%}

  {%- set exprs = {} -%}
  {%- for f in canonical_fields -%}
    {%- do exprs.update({f: []}) -%}
  {%- endfor -%}

  {%- if execute -%}
    {%- set rules_query -%}
      select source_column, rule_type, rule_param
      from {{ source('bus_rules', 'column_rules') }}
      where active and severity = 'reject' and source_system = '{{ source_system }}'
    {%- endset -%}
    {%- set results = run_query(rules_query) -%}
    {%- for row in results.rows -%}
      {%- set src_col, rule_type, rule_param = row[0], row[1], row[2] -%}
      {%- set canonical = canonical_column_map.get(src_col) -%}
      {%- if canonical is none or canonical not in canonical_fields -%}
        {#- Not requested by this call -- e.g. ERP's full_name, or a field
            this source_system's model handles by hand. Not an error: the
            caller explicitly opted in via canonical_fields. -#}
      {%- else -%}
        {%- set expr = rule_type_expr(rule_type, canonical, rule_param) -%}
        {%- if expr is none -%}
          {{ exceptions.raise_compiler_error(
              "column_rule_flags(" ~ source_system ~ "): unrecognized rule_type '" ~ rule_type ~
              "' for source_column '" ~ src_col ~ "' -- no branch in macros/rule_type_expr.sql. " ~
              "Add one there before this rule_type can be used."
          ) }}
        {%- else -%}
          {%- do exprs[canonical].append(expr) -%}
        {%- endif -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}

  {%- set parts = [] -%}
  {%- for f in canonical_fields -%}
    {%- if execute and (exprs[f] | length) == 0 -%}
      {{ exceptions.raise_compiler_error(
          "column_rule_flags(" ~ source_system ~ "): no active reject-severity bus_rules.column_rules " ~
          "row maps to canonical field '" ~ f ~ "' -- expected at least one. If this field genuinely " ~
          "has no rule for " ~ source_system ~ ", remove it from this macro call's canonical_fields " ~
          "list and hand-write its err_ column in the model instead (see this macro's docstring)."
      ) }}
    {%- endif -%}
    {%- do parts.append("(" ~ (exprs[f] | join(' or ')) ~ ") as " ~ err_column_name[f]) -%}
  {%- endfor -%}
  {{ return(parts | join(',\n    ')) }}
{% endmacro %}
