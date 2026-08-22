-- Phase 2 (domain-agnostic) replacement for silver_customers.sql. Silver =
-- canonical, validated records only, across every domain -- invalid records
-- never reach this table; they are routed to exceptions_queue for the
-- stewardship app instead. Attribute-bag shape (domain_key + source_record
-- identity + a JSON `attributes` bag) instead of a fixed Customer column
-- list, so this model needs no change to support a second live domain.
select
    domain_key,
    source_system,
    source_record_id,
    attributes,
    current_timestamp as silver_load_ts
from {{ ref('staged_records') }}
where not is_invalid
