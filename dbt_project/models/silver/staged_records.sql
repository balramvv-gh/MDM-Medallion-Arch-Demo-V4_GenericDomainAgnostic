-- Phase 2 (domain-agnostic) replacement for silver_all_staged.sql. Every
-- domain/source is already unioned together inside stg_source_records.sql
-- (see macros/active_domain_sources.sql), and is_invalid/reject_reasons are
-- already computed there too (macros/validation_block.sql) -- so this layer
-- is now just the logical-delete filter, kept as its own model (rather than
-- folded into stg_source_records.sql) to preserve the project's existing
-- three-layer staging -> staged -> {records, exceptions} shape and give a
-- clean seam for any future domain-independent staged-record logic.
select *
from {{ ref('stg_source_records') }}
-- Logical deletes never reach silver.records (or, downstream, the
-- exceptions queue): a deleted source record simply drops out of
-- matching/survivorship on the next full recompute, which is what lets a
-- golden record re-survive from its remaining active source (partial
-- delete) or retire entirely once every contributing source is gone (full
-- delete) -- both handled automatically by gold's existing
-- full-rebuild-from-silver logic, no gold-layer changes needed.
where not is_deleted
