-- Standardization applied per rules R006 (proper-case names), R007 (trim), R008 (phone -> E.164-ish)
-- Validation flags computed per rules R001-R005 (see seed: column_rules)
--
-- bronze.crm_customers_raw is append-only (scripts/load_bronze.py lands every
-- full/incremental load as new rows, never overwrites) -- so this model first
-- collapses it down to current state, the single latest row per
-- source_record_id by _load_ts, before standardizing anything. A full load
-- only ever produces one row per key, so this is a no-op in that case.
--
-- Incremental materialization: on a full-refresh run (or the first-ever
-- run, which dbt detects automatically since the target table doesn't
-- exist yet), `raw` scans bronze's entire history, exactly as before. On a
-- plain incremental run, `raw` is scoped to just the current bronze ingest
-- batch (macros/latest_load_batch.sql, reading bronze.load_batches) --
-- letting this model merge only what changed instead of rescanning every
-- row ever landed. unique_key + delete+insert means an incoming row
-- (including one with is_deleted=true) replaces that key's existing row
-- here outright; is_deleted itself is NOT filtered here -- silver_all_staged.sql
-- does that filtering downstream, on its own unchanged full rebuild, which
-- is what makes a deletion here propagate correctly to silver/gold with no
-- special-case logic (see PROJECT_KNOWLEDGE.md).
{{ config(materialized='incremental', unique_key='source_record_id', incremental_strategy='delete+insert') }}
with raw as (
    select * from {{ source('bronze', 'crm_customers_raw') }}
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
                partition by source_record_id
                order by _load_ts desc, _load_batch_id desc
            ) as _rn
        from raw
    )
    where _rn = 1
),

standardized as (
    select
        source_system,
        source_record_id,
        coalesce(try_cast(is_deleted as boolean), false) as is_deleted,
        -- R006/R007: trim + proper case
        nullif(trim(regexp_replace({{ proper_case('first_name') }}, '\s+', ' ')), '') as first_name,
        nullif(trim(regexp_replace({{ proper_case('last_name') }}, '\s+', ' ')), '') as last_name,
        nullif(trim(email), '') as email,
        -- R008: standardize phone to +1XXXXXXXXXX
        case
            when regexp_replace(coalesce(phone, ''), '[^0-9]', '', 'g') != ''
            then '+1' || right(regexp_replace(phone, '[^0-9]', '', 'g'), 10)
            else null
        end as phone,
        nullif(trim(address_line1), '') as address_line1,
        cast(null as varchar) as address_line2,
        nullif(trim(city), '') as city,
        upper(nullif(trim(state), '')) as state_code,
        nullif(trim(zip), '') as postal_code,
        upper(nullif(trim(country), '')) as country_code,
        try_cast(created_date as date) as source_created_date,
        try_cast(modified_date as date) as source_modified_date
    from latest
)

select
    *,
    -- Phase 2 (flexibility): first_name/last_name/email/state_code/country_code
    -- (R001-R005) are generated straight from active bus_rules.column_rules
    -- rows at compile time -- see macros/column_rule_flags.sql. phone is NOT
    -- included: CRM has no column_rules row for phone (R001-R005 only cover
    -- the other five), so it stays hand-written below, matching the
    -- pre-Phase-2 behavior exactly.
    {{ column_rule_flags('CRM', ['first_name', 'last_name', 'email', 'state_code', 'country_code']) }},
    (phone is null) as err_missing_phone
from standardized
