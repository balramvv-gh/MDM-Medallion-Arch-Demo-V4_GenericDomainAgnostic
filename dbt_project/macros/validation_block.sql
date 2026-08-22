{#
  Phase 2 (domain-agnostic) replacement for column_rule_flags.sql: generates
  `is_invalid` and `reject_reasons` for a (domain_key, source_system)
  directly from active bus_rules.column_rules rows, read at compile time via
  run_query. Two kinds of reject rule row are handled, both driven entirely
  by governed data -- no domain or source name is ever tested in this file:

    1. attribute_key IS NOT NULL: a plain "does this mapped attribute
       violate this rule" check (scripts/migrate_column_rules_to_domain.py's
       backfill). expr comes from rule_type_expr(rule_type, attribute_key,
       rule_param) -- same handler registry column_rule_flags.sql already
       used. Multiple active rules on the same attribute_key are OR'd
       together into one combined condition for that attribute.

    2. attribute_key IS NULL, rule_type = 'not_null': an ungoverned
       "composite" check left over from a source column that a
       source_field_mappings row SPLITS into more than one attribute (e.g.
       ERP's full_name -> first_name + last_name -- see
       scripts/migrate_column_rules_to_domain.py's R009 docstring). This
       macro resolves which attribute_keys were derived from that
       source_column by scanning active source_field_mappings rows whose
       source_expr mentions it (a plain substring match against the SQL
       expression text -- source_expr values are short, governance-owned
       strings, e.g. "split_part(attributes->>'full_name', ',', 2)"), and
       flags the record when ANY of those derived attributes ended up null.
       If no source_field_mappings row references that source_column at
       all, this is a genuine configuration gap and raises a compiler error
       rather than silently emitting no check for it.

  Both kinds produce a paired (condition, reason_text) entry; reason_text is
  "<rule_id>: <description>" straight from the governed rule row itself
  (steward-editable via Rules Configuration, not hardcoded label text here --
  see this project's Phase 2 note on why reject_reasons wording intentionally
  changed from pre-Phase-2's hand-typed strings).

  Returns a comma-separated block of exactly two columns:
      (<cond1> or <cond2> or ... or false) as is_invalid,
      list_filter([<reason1>, <reason2>, ...], x -> x is not null) as reject_reasons
  ("or false" / an all-null reason list are the safe empty-rule-set case --
  a domain/source with zero active reject rules is valid, not an error.)
#}
{% macro validation_block(domain_key, source_system) %}
  {%- set conditions = [] -%}
  {%- set reasons = [] -%}
  {%- if execute -%}
    {%- set rules_query -%}
      select rule_id, source_column, attribute_key, rule_type, rule_param, description
      from {{ source('bus_rules', 'column_rules') }}
      where active and severity = 'reject'
        and domain_key = '{{ domain_key }}' and source_system = '{{ source_system }}'
    {%- endset -%}
    {%- set rules = run_query(rules_query) -%}

    {#- Group attribute-mapped rules by attribute_key so multiple rules on
        the same attribute combine into one OR'd condition. -#}
    {%- set by_attribute = {} -%}
    {%- set composite_rows = [] -%}
    {%- for row in rules.rows -%}
      {%- set rule_id, source_column, attribute_key, rule_type, rule_param, description = row[0], row[1], row[2], row[3], row[4], row[5] -%}
      {%- if attribute_key is not none -%}
        {%- if attribute_key not in by_attribute -%}
          {%- do by_attribute.update({attribute_key: []}) -%}
        {%- endif -%}
        {%- do by_attribute[attribute_key].append((rule_id, rule_type, rule_param, description)) -%}
      {%- elif rule_type == 'not_null' -%}
        {%- do composite_rows.append((rule_id, source_column, description)) -%}
      {%- else -%}
        {{ exceptions.raise_compiler_error(
            "validation_block(" ~ domain_key ~ "." ~ source_system ~ "): rule " ~ rule_id ~
            " has no attribute_key and rule_type '" ~ rule_type ~ "' != 'not_null' -- only a composite " ~
            "not_null check (see this macro's docstring) is supported for an unmapped rule row."
        ) }}
      {%- endif -%}
    {%- endfor -%}

    {%- for attribute_key, rule_list in by_attribute.items() -%}
      {%- set attr_exprs = [] -%}
      {#- Jinja gotcha: {% set %} inside a {% for %} loop is scoped to that
          iteration and does NOT persist once the loop ends (unlike Python) --
          so the "first rule's reason text" can't be a plain scalar
          reassigned inside this loop. Collect candidates into a list (list
          mutation via .append() DOES persist, that's how attr_exprs above
          works) and take the first one after the loop instead. -#}
      {%- set attr_reason_candidates = [] -%}
      {%- for rule_id, rule_type, rule_param, description in rule_list -%}
        {%- set expr = rule_type_expr(rule_type, attribute_key, rule_param) -%}
        {%- if expr is none -%}
          {{ exceptions.raise_compiler_error(
              "validation_block(" ~ domain_key ~ "." ~ source_system ~ "): unrecognized rule_type '" ~
              rule_type ~ "' for attribute_key '" ~ attribute_key ~ "' (rule " ~ rule_id ~ ") -- no branch " ~
              "in macros/rule_type_expr.sql. Add one there before this rule_type can be used."
          ) }}
        {%- endif -%}
        {%- do attr_exprs.append(expr) -%}
        {%- do attr_reason_candidates.append("case when (" ~ expr ~ ") then '" ~ rule_id ~ ": " ~ (description | replace("'", "''")) ~ "' end") -%}
      {%- endfor -%}
      {%- set attr_reason = attr_reason_candidates[0] -%}
      {%- do conditions.append("(" ~ (attr_exprs | join(' or ')) ~ ")") -%}
      {%- do reasons.append(attr_reason) -%}
    {%- endfor -%}

    {%- if (composite_rows | length) > 0 -%}
      {%- set map_query -%}
        select attribute_key, source_expr
        from {{ source('governance', 'source_field_mappings') }}
        where active and domain_key = '{{ domain_key }}' and source_system_key = '{{ source_system }}'
      {%- endset -%}
      {%- set mappings = run_query(map_query) -%}
      {%- for rule_id, source_column, description in composite_rows -%}
        {%- set derived = [] -%}
        {%- for m in mappings.rows -%}
          {%- if source_column in m[1] -%}
            {%- do derived.append(m[0]) -%}
          {%- endif -%}
        {%- endfor -%}
        {%- if (derived | length) == 0 -%}
          {{ exceptions.raise_compiler_error(
              "validation_block(" ~ domain_key ~ "." ~ source_system ~ "): rule " ~ rule_id ~
              " (source_column='" ~ source_column ~ "', no attribute_key) has no active " ~
              "governance.source_field_mappings row whose source_expr references '" ~ source_column ~
              "' -- it can neither auto-generate as a plain attribute check (attribute_key is null) " ~
              "nor resolve as a composite split check. Map an attribute_key onto it, or point its " ~
              "source_column at a raw field a source_field_mappings row actually derives from."
          ) }}
        {%- endif -%}
        {%- set null_checks = [] -%}
        {%- for d in derived -%}
          {%- do null_checks.append(d ~ " is null") -%}
        {%- endfor -%}
        {%- set cond = "(" ~ (null_checks | join(' or ')) ~ ")" -%}
        {%- do conditions.append(cond) -%}
        {%- do reasons.append("case when " ~ cond ~ " then '" ~ rule_id ~ ": " ~ (description | replace("'", "''")) ~ "' end") -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endif -%}

  {%- set is_invalid_expr = (conditions | join(' or ')) if (conditions | length) > 0 else 'false' -%}
  {%- set reasons_list = (reasons | join(', ')) if (reasons | length) > 0 else '' -%}
  {{ return(
      "(" ~ is_invalid_expr ~ ") as is_invalid,\n    list_filter([" ~ reasons_list ~ "], x -> x is not null) as reject_reasons"
  ) }}
{% endmacro %}
