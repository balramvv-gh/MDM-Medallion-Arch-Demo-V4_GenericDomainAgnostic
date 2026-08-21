-- Phase 2 (domain-agnostic): records that failed one or more validation
-- rules land here for a data steward to review, remediate (with AI assist),
-- and re-submit into the pipeline. Attribute-bag shape, same as records.sql
-- -- works unchanged for any domain.
select
    md5(domain_key || '-' || source_system || '-' || source_record_id) as exception_id,
    domain_key,
    source_system,
    source_record_id,
    attributes,
    reject_reasons,
    'open' as remediation_status,   -- open | in_review | resolved | rejected
    current_timestamp as queued_ts
from {{ ref('staged_records') }}
where is_invalid
