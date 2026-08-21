{#
  Compile-time lookup of "the current bronze ingest batch", read from
  bronze.load_batches (append-only, one row per scripts/load_bronze.py
  invocation -- see that script's docstring). Same "query a DB-native table
  at compile time via run_query" pattern already used by
  column_rule_flags.sql (against bus_rules.column_rules).

  Used by stg_crm_customers.sql / stg_erp_customers.sql's incremental
  branch to scope bronze to just the rows landed by the most recent load,
  instead of rescanning bronze's entire history on every incremental run.
  build_pipeline.py separately reads bronze.load_batches itself to decide
  whether to pass --full-refresh to the silver dbt run (a full/mixed batch
  needs a genuine full-refresh -- see that table's column doc in
  models/bronze/sources.yml -- not an incrementally-scoped merge).

  latest_load_batch_id(): returns the load_batch_id of the most recent row
  in bronze.load_batches (by started_at), or none if the table doesn't
  exist yet or has no rows (e.g. a fresh clone before bronze has ever been
  loaded -- callers must handle that case, they should never be asking for
  a batch id at all if bronze itself is empty).
#}
{% macro latest_load_batch_id() %}
  {%- set result = none -%}
  {%- if execute -%}
    {%- set relation = adapter.get_relation(database=target.database, schema='bronze', identifier='load_batches') -%}
    {%- if relation is not none -%}
      {%- set batch_query -%}
        select load_batch_id
        from {{ source('bronze', 'load_batches') }}
        order by started_at desc
        limit 1
      {%- endset -%}
      {%- set results = run_query(batch_query) -%}
      {%- if (results.rows | length) > 0 -%}
        {%- set result = results.rows[0][0] -%}
      {%- endif -%}
    {%- endif -%}
  {%- endif -%}
  {{ return(result) }}
{% endmacro %}
