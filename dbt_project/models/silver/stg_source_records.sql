-- Phase 2 (domain-agnostic) replacement for stg_crm_customers.sql /
-- stg_erp_customers.sql: ONE generic staging model for every domain and
-- source system, instead of one hand-written dbt file per source. Which
-- domain/source combos exist comes from macros/active_domain_sources.sql
-- (governance.domain_source_systems); standardization comes from
-- macros/mapped_attributes.sql (governance.source_field_mappings); reject
-- validation comes from macros/validation_block.sql (bus_rules.column_rules).
-- No domain or source name appears anywhere in this file -- onboarding a new
-- domain/source is purely a metadata change, never a change here.
--
-- bronze.raw_records is append-only (scripts/load_bronze.py lands every
-- full/incremental load as new rows, never overwrites) -- so this model
-- first collapses it down to current state, the single latest row per
-- (domain_key, source_system, source_record_id) by _load_ts, before
-- standardizing anything. A full load only ever produces one row per key,
-- so this is a no-op in that case.
--
-- Incremental materialization: on a full-refresh run (or the first-ever
-- run, which dbt detects automatically since the target table doesn't exist
-- yet), `raw` scans bronze's entire history, exactly as before Phase 2. On a
-- plain incremental run, `raw` is scoped to just the current bronze ingest
-- batch (macros/latest_load_batch.sql, reading bronze.load_batches) --
-- letting this model merge only what changed instead of rescanning every
-- row ever landed. unique_key + delete+insert means an incoming row
-- (including one with is_deleted=true) replaces that key's existing row
-- here outright; is_deleted itself is NOT filtered here -- the next layer
-- (staged_records.sql) does that filtering downstream, on its own
-- unchanged full rebuild, which is what makes a deletion here propagate
-- correctly to silver/gold with no special-case logic.
{{ config(materialized='incremental', unique_key=['domain_key', 'source_system', 'source_record_id'], incremental_strategy='delete+insert') }}

{% set combos = active_domain_sources() %}
{% if execute and (combos | length) == 0 %}
  {{ exceptions.raise_compiler_error(
      "stg_source_records: governance.domain_source_systems has no active rows for any active domain -- " ~
      "nothing to build. Run scripts/ensure_domain_metadata.py, or check domain_registry.is_active / " ~
      "domain_source_systems.active."
  ) }}
{% endif %}

with raw as (
    select * from {{ source('bronze', 'raw_records') }}
    {% if is_incremental() %}
    where _load_batch_id = '{{ latest_load_batch_id() }}'
    {% endif %}
),

latest as (
    select * exclude (_rn)
    from (
        select
            *,
            row_number() over (
                partition by domain_key, source_system, source_record_id
                order by _load_ts desc, _load_batch_id desc
            ) as _rn
        from raw
    )
    where _rn = 1
)

{% for domain_key, source_system in combos %}
, standardized_{{ loop.index }} as (
    select
        domain_key,
        source_system,
        source_record_id,
        is_deleted,
        _load_ts,
        _load_batch_id,
        {{ mapped_attributes(domain_key, source_system) }}
    from latest
    where domain_key = '{{ domain_key }}' and source_system = '{{ source_system }}'
)
, validated_{{ loop.index }} as (
    select
        domain_key,
        source_system,
        source_record_id,
        is_deleted,
        _load_ts,
        _load_batch_id,
        {{ pack_attributes_json(domain_key) }} as attributes,
        {{ validation_block(domain_key, source_system) }}
    from standardized_{{ loop.index }}
)
{% endfor %}

{% for domain_key, source_system in combos %}
select * from validated_{{ loop.index }}
{% if not loop.last %}union all by name{% endif %}
{% endfor %}
