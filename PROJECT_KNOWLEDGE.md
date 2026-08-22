# MDM Medallion Architecture Demo — Project Knowledge

Portfolio project for Balram's job search (targeting Senior MDM Architect / Director-level
data governance roles). Working demo, not production software — built to be a genuine
interview talking piece with runnable code behind it.

**This is V4**: the V3 codebase (single hardcoded "Customer" domain) was generalized into
a domain-agnostic platform — see "V4: domain-agnostic generalization" below for what
changed and why. For most of V4's life, only the **Customer** domain had a real,
tested, end-to-end pipeline, with a second domain, **Product**, sitting in as
metadata-only (`domain_registry`/`domain_relationships` rows, no attributes, no source
systems, no gold data) just to prove the platform wasn't secretly still hardcoded to
one domain. **Product has since been onboarded for real** — three source systems, a
20-attribute schema, validation/matching/survivorship rules, a synthetic data
generator, and a verified end-to-end pipeline — using the exact same additive,
metadata-driven onboarding process V4 was built to support: new governance rows, two
genuinely new rule types the generic platform code didn't have a branch for yet, one
new file, and one incidental bug fix in unrelated pre-existing code. See "Product
domain onboarding" below.

## Purpose & positioning

Demonstrates hands-on MDM architecture skills: metadata-driven data quality, match/merge
and survivorship, data stewardship workflows with AI assistance, lineage/impact analysis,
access-controlled data products, and a genuinely multi-domain platform architecture — the
kinds of capabilities a Senior MDM Architect or data governance Director is expected to
have designed firsthand, not just managed. The domain-agnostic generalization in particular
mirrors how real multi-domain MDM platforms (Informatica MDM, Reltio) model domains: a
generic attribute-bag schema plus a metadata registry, not one bespoke schema per domain.

## V4: domain-agnostic generalization

V3 was hardwired to a single domain (Customer): fixed bronze/silver/gold table
schemas with named Customer columns, "CRM"/"ERP" hardcoded as source labels,
"Browse Customers" hardcoded in the UI, a single global `gold_access` column, and
column/matching/survivorship rules keyed by fixed column names. V4 generalizes this
so the same pipeline, rules engine, stewardship workflow, and portal work for any
domain — Customer, Product, Supplier, Location, whatever — with **no domain name
hardcoded anywhere in code**. Which domains exist, what fields each has, which
source systems feed them, how domains relate to each other, and who may access
them are all governed metadata now, not code.

**New governance/metadata tables** (schema `governance`):
- `domain_registry` — domain_key (PK), display_name, entity_label_singular/plural,
  description, pipeline_status (`live`/`planned`), is_active, sort_order. Replaces
  every hardcoded "Customer"/"customer" string in UI copy and code.
- `domain_attributes` — domain_key, attribute_key, display_label, data_type,
  is_required, is_matching_field, is_similarity_text_field, is_blocking_key,
  is_dashboard_breakdown, sort_order, active. Drives dynamic form rendering,
  validation, matching-rule targets, and dashboard breakdown panes.
- `domain_source_systems` — domain_key, source_system_key, display_name,
  sort_order, active. Replaces the hardcoded "CRM"/"ERP" labels; sort_order feeds
  survivorship's tie-break (replacing the old hardcoded "CRM preferred").
- `source_field_mappings` — domain_key, source_system_key, attribute_key,
  source_expr (a DuckDB expression against the bronze `attributes` JSON, e.g.
  `attributes->>'email_addr'`), transform_function, active. Replaces the old
  per-source dbt staging models (`stg_crm_customers.sql`/`stg_erp_customers.sql`)
  with one generic staging model driven by this table.
- `domain_relationships` — relationship_id, domain_a_key, domain_b_key,
  relationship_label, description, is_active. Powers the Home page diagram's
  distinctly-styled link nodes (today: one row, "Customer-Product link").
- `user_domain_access` — user_id, domain_key, gold_access
  (`read_write`/`read`/`none`). Replaces the single global `auth.users.gold_access`
  column; a user can have different access to different domains. The legacy
  column still exists (used by the two bootstrap user-creation scripts and shown
  by `GET /api/v1/auth/me`) but `api/auth.py`'s `domain_access_level()` — the
  single source of truth every domain-scoped endpoint actually checks — reads
  only `user_domain_access`, with no fallback to the legacy column. A user has no
  implicit access to a domain they have no row for, even with a `read_write`
  legacy `gold_access`. `scripts/ensure_domain_metadata.py`'s
  `migrate_user_domain_access()` additively backfills a `customer`-domain row for
  any user who doesn't have one yet (run every `build_pipeline.py`, and mirrored
  at API startup via `api/db.py`'s `_ensure_auth_tables`), so an account created
  via the legacy path still works normally against Customer.

**Generalized physical schema** (attribute-bag, replacing V3's fixed customer-typed
tables):
- `bronze.raw_records(domain_key, source_system, source_record_id, attributes JSON,
  source_modified_date, is_deleted, _load_ts, _load_batch_id, ...)` — one shared
  table for every domain/source, replacing V3's separate CRM/ERP landing tables.
  `bronze.load_control`/`load_batches` are `domain_key`-scoped.
- `silver.stg_source_records` — one generic incremental dbt model (unique_key
  `[domain_key, source_system, source_record_id]`) replacing the old per-source
  `stg_crm_customers.sql`/`stg_erp_customers.sql`. Standardizes every active
  domain/source combination's raw fields into that domain's canonical attribute
  keys (`macros/mapped_attributes.sql`, driven by `source_field_mappings`) and
  packs them into an `attributes` JSON bag; validation (`macros/validation_block.sql`,
  driven by `bus_rules.column_rules`) is computed in the same pass. Which
  domain/source combos this model builds for comes entirely from
  `macros/active_domain_sources.sql`, itself driven by `domain_source_systems`.
- `silver.staged_records` — the logical-delete filter, kept as its own model (not
  folded into `stg_source_records`) to preserve the project's three-layer
  staging → staged → {records, exceptions} shape.
- `silver.records` / `silver.exceptions_queue` — canonical/invalid records, same
  attribute-bag shape, split on `is_invalid` (computed upstream).
- `gold_prep.match_groups/match_edges/match_review_candidates` (written by
  `scripts/generate_matches.py`, unchanged Python step, now looping over every
  domain gold_prep has silver data for) → `gold.gold_match_candidates` (silver
  records joined to their match_group_id) → `gold.gold_survivorship_winners`
  (attribute-level survivorship, generalized from a fixed 10-column pivot to every
  active `domain_attributes` row of every domain) → `gold.golden_records`
  (pivoted back to one row per golden record's `attributes` JSON) and
  `gold.gold_crosswalk` (per-source `winning_attributes`) →
  `gold.gold_match_review_queue`.
- **Golden IDs are domain-qualified**: `'GOLD-' || upper(domain_key) || '-' ||
  lpad(match_group_id, 5, '0')`, e.g. `GOLD-CUSTOMER-00001`, replacing V3's flat
  `GOLD-00001` — a deliberate, necessary breaking format change, since
  `match_group_id` is only unique within one domain's own matching run (a second
  live domain would otherwise produce colliding IDs). Flagged explicitly in
  `golden_records.sql`'s own docstring.
- `bus_rules.column_rules`/`matching_rules`/`matching_thresholds`/
  `survivorship_rules` all gained a `domain_key` column (and, where they used to
  reference a fixed column name, an `attribute_key` column instead) via two
  one-time, idempotent migration scripts (`scripts/migrate_column_rules_to_domain.py`,
  `scripts/migrate_matching_rules_to_domain.py`) that run on every
  `build_pipeline.py` invocation, before `dbt seed`/`dbt run` — the generic
  silver/gold models assume `domain_key` already exists on these tables.
- `stewardship.*` tables (`remediation_log`, `remediated_records`,
  `exception_status_overrides`, `match_review_overrides`, `match_review_log`) are
  unchanged in shape but now carry whatever `domain_key` their parent
  exception/pair belongs to. **Renamed from the V3-era `stewardship_app` schema
  name to `stewardship`** (the frontend's `LAYER_RANK` lineage-graph constant
  still recognizes both names for backward compatibility, though nothing writes
  to `stewardship_app` anymore).

**Pipeline code**: `scripts/generate_matches.py`, `api/validation.py`, and
`api/reprocessing.py`'s survivorship ranking all read *which* attributes to
validate/match/rank from `domain_attributes`/`bus_rules.*` metadata per domain,
instead of a fixed field list — extending the existing handler-registry pattern
(`RULE_TYPE_HANDLERS`, `SURVIVORSHIP_RANK_HANDLERS`, `TRANSFORM_FUNCTIONS`) rather
than inventing a new one.

**API**: every record/dashboard/lineage-adjacent endpoint takes `domain_key` as a
path parameter (`/api/v1/records/{domain_key}/...`,
`/api/v1/portal/dashboard/{domain_key}/summary`), and `api/auth.py`'s
`require_domain_read`/`require_domain_write` FastAPI dependencies resolve
`domain_key` from the route's own path parameter (shared by name across a route's
dependencies) and check `user_domain_access`. `_require_known_domain`/
`_require_live_domain` (`api/main.py`) uniformly 404 an unknown/inactive
`domain_key` and 409 one with no pipeline yet, so a typo'd or not-yet-built domain
fails loudly and consistently rather than each endpoint silently querying empty
tables.

**Frontend**: a new post-login **Home page** (a pannable/zoomable relationship
diagram, reusing the Lineage graph's rendering pattern) whose nodes are every
active domain and whose distinctly-styled link nodes are active
`domain_relationships` rows; clicking a domain the user has access to and that has
a live pipeline enters its dashboard, a domain without access toasts and stays on
Home, and a `planned` domain shows "not yet available" (access-lock takes
priority over pipeline-status when both apply). Existing screens ("Browse
Customers" → "Browse `<Domain Plural Label>`", dashboards, record detail/edit
forms) get their labels and field lists from `domain_registry`/`domain_attributes`
at render time instead of hardcoded HTML/JS, the same way Rules Configuration
already rendered generically off `entity_type_registry`.

**Verified end-to-end this pass** (maker-checker approval-flow testing, real
Playwright browser automation against a running API + both frontends, 25/25
checks): `testadmin` submits a gold-record edit and a domain-access grant;
`steward1` resolves an exception and confirms a match; `owner1` approves the
gold-edit's first quorum slot (Portal) plus a `stewardship_remediation` and a
`match_review_confirmation` step-1 (Stewardship console); `owner2` completes the
gold-edit's second quorum slot (Portal); `admin2` approves the
`match_review_confirmation` step-2 and drains the `user_admin_change` category
(Portal) — followed by API-level verification of every resulting DB mutation.
Two real gaps surfaced and were fixed as part of this testing, not deferred:
- `match_review_confirmation`'s step 2 needs an `admin` decision, but the
  Stewardship console (where this workflow type is normally shown) is role-gated
  to `dataSteward`/`dataOwner` only — an admin had no screen to ever reach that
  pending decision. Fixed by extending the **Portal's** Approvals category
  visibility to admins specifically for this one workflow type
  (`visiblePortalWorkflowTypes()`/`approvalCategories()` in
  `portal_app/frontend/index.html`), since the Portal is the only app an admin
  can log into.
- The Match Review confirm button in `stewardship_app/frontend/index.html` read
  "Confirm — same customer, merge" — hardcoded, not the finding this looks like
  at first glance, but a real gap in the *other* direction from what was
  hardcoded on the domain side. Fixed to read the domain's own
  `entity_label_singular` (`domainEntityLabelSingular()`), so it now correctly
  says "same customer" for the Customer domain specifically because that's the
  governed metadata value, and would say "same product"/whatever for another
  domain without a code change.

## Architecture

Medallion architecture (bronze/silver/gold) with two synthetic heterogeneous source
systems (a CRM and an ERP) for the Customer domain, so schema standardization is a
real problem, not a formality — see "V4: domain-agnostic generalization" above for
the generic attribute-bag shape every domain now shares.

- **Bronze** — `bronze.raw_records`, one shared table for every domain/source,
  tagged with domain_key/source_system/source_record_id. Append-only (every load,
  full or incremental, inserts new rows rather than overwriting) —
  `bronze.load_control` (domain_key, source_system, load_type, watermark_date,
  last_loaded_at, row_count) tracks each domain+source's load history and drives
  auto-detection: no control row yet = full load, a control row exists =
  incremental (filtered to that domain's modified-date attribute ≥ the
  watermark). `scripts/load_bronze.py --force-full` overrides detection. A
  separate append-only log, `bronze.load_batches` (one row per `load_bronze.py`
  invocation, never updated — same convention as `audit.audit_trail`/
  `stewardship.match_review_log`), records the UUID shared by every row that run
  landed (`_load_batch_id`), the run's overall `load_type`, which domain+source
  combos it touched, and total row count. `silver.stg_source_records` is a dbt
  `incremental` model (`unique_key=[domain_key, source_system, source_record_id]`,
  `incremental_strategy='delete+insert'`): on a full-refresh run it collapses
  bronze's entire history to one current row per key (latest `_load_ts`), exactly
  as before; on a plain incremental run it merges only the current batch
  (`macros/latest_load_batch.sql` reads `bronze.load_batches` at compile time to
  know which rows belong to it), instead of rescanning every row bronze has ever
  landed. `scripts/build_pipeline.py` reads `bronze.load_batches` itself to
  decide whether to pass `--full-refresh` to that dbt run. Logical deletes
  (`is_deleted`, sourced from `data/generate_source_data.py --incremental`) are
  carried through the staging layer unfiltered and filtered out downstream in
  `staged_records.sql`, which (along with `records.sql`, `exceptions_queue.sql`,
  and every gold model) stays a full `table` rebuild on every run; that's what
  makes deletion-propagation and match-group recompute work correctly with zero
  special-case code — a deleted source record drops out of matching/survivorship
  on the next full recompute, letting a golden record re-survive from its
  remaining source (partial delete) or retire entirely once every contributing
  source is gone (full delete). This two-tier design (incremental staging,
  full-rebuild everything downstream) is deliberate: at this project's data
  volumes, only the bronze rescan was worth avoiding — see Known simplifications
  for why gold's own full-rebuild-every-run architecture, and the fuzzy tier's
  matching algorithm, are enterprise-scale limitations, not fixed by this change.
- **Silver** — canonical, validated records only, across every domain.
  Cleansing/standardization is driven by `governance.source_field_mappings`
  (per domain+source+attribute); validation is **metadata-driven**: rules live in
  `bus_rules.column_rules` (rule_id, domain_key, source_system, attribute_key,
  rule_type, severity) — a DB-native table maintained via the Data Governance >
  Rules Configuration screen's maker-checker workflow. Records failing validation
  route to an exception queue instead of silver.
- **Gold (Data Hub)** — hybrid match/merge across sources, and **fully
  metadata-driven**, domain-scoped throughout: tier definitions/thresholds live in
  `bus_rules.matching_thresholds` and the attributes each tier operates on live
  in `bus_rules.matching_rules` (a child table keyed by tier_id) — nothing about
  *what* matches is hardcoded in pipeline code. Today's Customer configuration has
  two tiers: tier 1 is deterministic exact match on normalized email OR phone
  (confidence = that tier's `auto_merge_threshold`, 1.00); tier 2 is an
  embedding-similarity fuzzy match (TF-IDF character n-gram cosine similarity over
  name/address text, blocked by state_code) that catches near-duplicates tier 1
  misses. Product's own configuration (added when it was onboarded — see "Product
  domain onboarding" below) follows the identical two-tier shape: tier 1 exact match
  on a normalized part number, tier 2 fuzzy match on product/manufacturer name text
  blocked by category. High-confidence fuzzy matches auto-merge with their similarity score as
  confidence; borderline ones surface in a **Match Review queue** for a data
  steward to confirm or reject rather than auto-merging. A third, non-tier row
  (`is_match_tier=false`) holds the 0.50 "provisional" baseline confidence
  assigned to a single-source golden record with no corroborating match at all —
  read by both the batch gold layer and real-time reprocessing, so they can't
  disagree on it. Computed in `scripts/generate_matches.py`, which runs as a
  Python step between silver and gold (dbt-duckdb can't do the embedding math in
  SQL) and writes to a `gold_prep` schema that the gold dbt models consume as a
  source — same "Python loads, dbt treats it as a source" pattern also used for
  bronze and for `ref`/`bus_rules` themselves. **Survivorship is
  attribute-level**, not record-level: for every active attribute of every
  domain, each is independently won by whichever contributing source best
  satisfies that attribute's own `bus_rules.survivorship_rules` rule
  (`most_common` / `most_complete` / `oldest` / `newest` / `pattern_match` /
  `shortest_value` / `source_priority` — the last added for Product, see "Product
  domain onboarding" below), with `source_modified_date` desc (then
  `domain_source_systems.sort_order`, replacing V3's hardcoded "CRM preferred")
  as the universal, non-configurable tie-break whenever a rule doesn't produce a
  clean winner — see `dbt_project/models/gold/gold_survivorship_winners.sql`. A
  **crosswalk table** (`gold.gold_crosswalk`) preserves the relationship between
  every golden record and its contributing source records, with a graduated
  match confidence score plus a `winning_attributes` list per source showing
  exactly which gold attributes that source won (`is_survivor_record` is simply
  "won at least one attribute").

## Tech stack

- **dbt-core + DuckDB** for the batch pipeline — genuinely runs locally, zero cloud cost,
  no Databricks account needed. This was a deliberate choice over Talend (which the user
  has production experience with) because Talend's MDM Server product and Open Studio are
  both end-of-life/discontinued as of 2024, and Databricks/dbt-style lakehouse patterns are
  what current job postings ask for.
- **scikit-learn** (TF-IDF + cosine similarity) for the fuzzy-matching tier — chosen over
  a neural sentence-embedding model (e.g. sentence-transformers) to keep the demo's setup
  fast and fully offline (no model download, no torch, no GPU); a defensible "real
  embedding-similarity technique" story for an interview without the install weight.
- **Faker** generates the ~100-row-per-source synthetic Customer-domain population (see Key
  data model notes below for how single-source, exact-duplicate, and fuzzy-duplicate cases
  are seeded).
- **FastAPI** backend for all apps and the REST API, with bcrypt-hashed password auth
  (from-scratch bearer-token sessions, 8-hour expiry).
- Plain HTML/JS frontends (no build step, no Node.js needed) for both apps — every field
  list, label, and form is rendered dynamically from `domain_registry`/`domain_attributes`,
  not hardcoded HTML.
- **Windows-first**: every file path is computed via `pathlib` relative to file location —
  no hardcoded absolute paths anywhere. The user runs this on Windows (PowerShell), the
  dev/test environment used to build it is Linux, so this portability was hard-won and
  tested by literally extracting into a space-containing path (`MDM Pipeline`) and running
  the full pipeline before shipping each zip.
- **MCP (Model Context Protocol) + croniter** (`mcp>=1.9,<2.0`, pinned below the SDK's 2.0
  server-API redesign) for the agent orchestration layer — see "MCP Agent Orchestration"
  below. Still dbt-core + DuckDB doing the actual pipeline work; MCP only adds a stateless
  agent/tool-registry layer on top, it does not change the execution engine. "Databricks"
  has come up in planning conversations as narrative/enterprise-pattern positioning for this
  layer, not a literal migration target — the zero-cloud-cost rationale above still holds.

## MCP Agent Orchestration

The batch pipeline (`scripts/build_pipeline.py`'s steps) is also available as seven
stateless agents (`agents/`) coordinated by an MCP server acting as the central orchestrator
and tool registry (`mcp_server/`, 11 tools). Full design rationale, the data model, and
end-to-end verification results are in `MCP_Agent_Orchestration_Design.docx` — this section
is a summary, not the source of truth. Domain-agnostic throughout — none of the agent or
tool-registry code names a domain; every agent just wraps whatever `build_pipeline.py`
already does across every active domain.

- **Ingestion / DQ / Matching / Survivorship agents** (`agents/ingestion_agent.py`,
  `dq_agent.py`, `matching_agent.py`, `survivorship_agent.py`) are thin, stateless wrappers
  around the exact same scripts/dbt commands `build_pipeline.py` already runs — no pipeline
  logic is reimplemented. `cancel_stale_match_reviews.py` runs inside Matching Agent (not a
  separate stage) and `audit_pipeline_diff.py` runs inside Survivorship Agent, both because
  `build_pipeline.py`'s own docstring requires them to run at those exact points for
  correctness. **Two stale-reference bugs found and fixed this pass**: `DQAgent._silver_row_counts()`
  queried the non-existent `main_silver.silver_customers` (real table:
  `main_silver.records`, since Phase 2) and `SurvivorshipAgent._gold_row_count()` queried
  the non-existent `main_gold.gold_customers` (real table: `main_gold.golden_records`,
  since Phase 3) — both left over from the V3→V4 rename, both silently swallowed by an
  `if exists else None` guard, so the health-check `detail` string always showed "? rows"
  instead of the real count. Fixed to query the real tables; verified directly against the
  live DB (185 silver rows, 16 exceptions queued, 110 golden records — matching known live
  counts).
- **Steward Agent** (`agents/steward_agent.py`) has two tiers: a read-only batch-time queue
  health snapshot (open exceptions / pending Match Review / pending approvals — genuinely
  stateless, safe on a schedule) plus real-time delegate methods to `api/reprocessing.py`'s
  `reprocess_corrected_record`/`reprocess_confirmed_match_review`. The delegates are NOT yet
  called from `api/main.py` — that still calls `api/reprocessing.py` directly, unchanged —
  wiring that through MCP is tracked as follow-up work, deliberately not bundled in.
- **Monitoring Agent** (`agents/monitoring_agent.py`) is net-new: SLA/DQ/health metrics
  graded against `governance.monitoring_thresholds` (DB-native, metadata-driven, same
  seed-if-empty convention as `bus_rules.*`).
- **Orchestration Agent** (`agents/orchestration_agent.py`) is the 7th agent — runs
  Ingestion → DQ → Matching → Survivorship → Steward in order, on a schedule read from
  `governance.orchestration_schedule` (metadata-driven: an OS scheduler only needs to invoke
  `python -m agents.orchestration_agent --serve` frequently, e.g. every 15 min; the actual
  cron cadence lives in the DB row, not the OS scheduler config). `--run-now` triggers an ad
  hoc run. Every run is logged, append-only, to `governance.orchestration_run_log`.
- `mcp_server/tool_registry.py` is the canonical catalog (11 tools); `mcp_server/server.py`
  registers them with the official `mcp` SDK's FastMCP server (stdio transport by default).
- **Not yet done**: `api/main.py` wired through the MCP tool registry instead of importing
  `api/reprocessing.py` directly; `orchestration_schedule`/`monitoring_thresholds` exposed
  via the Rules Configuration screen; an actual OS-level Task Scheduler entry for the polling
  loop (today the pipeline has only been triggered manually via `--run-now`).

## Applications

1. **Data Stewardship console** (`/app/`) — four tabs, scoped to one selected domain
   at a time (`stewardshipDomain`, carried over from the portal's `currentDomain` via
   a `?domain=` query param, or picked from the console's own Home
   relationship-diagram tab when opened directly — see "Stewardship console
   navigation & domain-scoping" below), fully metadata-driven for whichever domain is
   currently active.
   - **Dashboard** (landing tab on login) — one screen, two panes: Exceptions and
     Match Review. Each pane has KPI cards for its status breakdown (Exceptions:
     open/in_review/resolved/rejected; Match Review: pending/in_review/confirmed/
     rejected — Total is a sum, not a category, and isn't itself clickable), a
     bar chart (Exceptions: by rejection reason; Match Review: similarity-score
     buckets computed live off `bus_rules.matching_thresholds`, not hardcoded), a
     pie chart for Match Review's status breakdown, a "Your Activity" row
     (Submitted/Approved/Pending/Sent back, scoped to the logged-in user via
     `governance.workflow_instances.maker_user_id`, and — since this pass — also
     scoped to the selected domain via `_my_workflow_activity`'s new `domain_key`
     param; see "Stewardship console navigation & domain-scoping" below), and a
     trend chart (Exceptions:
     created vs. resolved vs. rejected per day). Every element except the trend
     chart is clickable and navigates to the Exception Queue or Match Review tab,
     pre-filtered to exactly that slice — a chart's count and its drill-through
     list can never disagree, since both read the same enriched view. Items in a
     filtered list that aren't `open` (Exceptions) or `pending` (Match Review)
     render read-only in the detail panel: no input fields, no action buttons,
     just a "Read-only · status: X" badge.
   - **Exception Queue** — reviews the exception queue for whichever domain(s) the
     steward can access, gets an AI-assisted remediation suggestion (real Claude
     API call via `ANTHROPIC_API_KEY`, with a domain-aware heuristic fallback —
     `api/ai_remediation.py`'s `HEURISTIC_FALLBACK_DOMAINS` gates the
     email/state/country pattern heuristics to the Customer domain specifically,
     since they only make sense for that field set; every other domain gets a
     generic "needs manual review" response from the fallback path, while the AI
     path itself works from whatever attribute keys the record/reject_reasons
     actually contain, for any domain, using that domain's own `entity_label` in
     the prompt), and resolves/rejects corrections. Resolving or rejecting
     submits into the `stewardship_remediation` maker-checker workflow (1 Data
     Owner approver, different from the steward) instead of applying immediately.
     Only once approved does resolving actually trigger real-time reprocessing:
     the corrected record is upserted into silver, matched against the current
     gold set for that domain using the same tier-1 `exact_match_field` rules the
     batch pipeline reads (exact tier only, see known simplifications), and
     survivorship is re-run to either update the matched golden record or create
     a new one within that domain; a rejected approval returns the exception to
     the open queue.
   - **Match Review** — borderline fuzzy-match candidates from the gold layer's
     embedding-similarity tier, shown side by side (using the domain's own
     `entity_label_singular` in the confirm button, e.g. "Confirm — same
     customer, merge" for Customer — see the domain-agnostic generalization
     section above for how this was fixed from a hardcoded "customer" string),
     for the steward to confirm or reject. Confirm/reject submits into a
     maker-checker workflow (`match_review_confirmation`, 2 sequential levels:
     Data Owner, then Admin). Once a **confirm** decision is approved, it
     auto-processes in real time (`api/reprocessing.py`'s
     `reprocess_confirmed_match_review`): attribute-level survivorship is re-run
     across the UNION of every source record already contributing to EITHER
     side's current golden record (within that domain) — producing the merged
     group's surviving value for every gold attribute except `golden_id` itself;
     the smaller (by numeric suffix) of the two sides' `golden_id`s survives; and
     the other is retired (logged to the audit trail as `logically_deleted`),
     with every crosswalk row that pointed at either golden record rewritten to
     point at the surviving one. A **reject** decision has nothing to
     reprocess — it only permanently excludes the pair from future matching
     runs. **Match Review hold**: while a pair's review is open, both of its two
     silver records sit out of matching entirely on every subsequent
     `generate_matches.py` run. If an updated version of either side's source
     record lands in silver while the review is still open, the review is
     auto-cancelled (status `cancelled`, a system-written note) rather than left
     to keep judging stale data — triggered in real time by
     `api/reprocessing.py`'s `_cancel_stale_match_reviews` and in the batch
     pipeline by `scripts/cancel_stale_match_reviews.py`.
   - **Approvals** — a split-pane view (queue list left, detail panel right)
     listing workflow instances awaiting this user's decision ("Awaiting my
     decision") or previously submitted by them ("My submissions"), for any of
     the six workflow types whose current step matches their role. The list is
     grouped by category (workflow type label) then sorted by instance ID, and a
     category `<select>` above the list narrows it to one workflow type at a
     time. Selecting a row shows the submitter's name, then an embedded,
     read-only view of the actual record the workflow concerns — for
     `stewardship_remediation`/`match_review_confirmation` this reuses the exact
     same rendering as the Exception Queue/Match Review tabs; the other four
     workflow types get an equivalent field-grid built from a per-type field
     list. Any field the submission actually changes is highlighted (darker
     green field background) with the prior value shown below in red,
     `Was: {old value}`; a create action shows a "New" badge instead.

2. **MDM Data Hub Portal** (`/portal/`) — login-gated, SSO'd with the Stewardship
   app. Roles are `admin` / `dataSteward` / `dataOwner` / `businessUser`; access to
   each active domain is `read_write` / `read` / `none` (per-domain, via
   `governance.user_domain_access` — see the domain-agnostic generalization
   section above). `admin` is a distinct governance function (user administration
   only) and is deliberately **not** a superset of stewardship rights — only
   `dataSteward`/`dataOwner` can reach the Stewardship console or its API
   endpoints. Has:
   - **Home** (the landing screen after login) — a pannable/zoomable relationship
     diagram of every active domain plus distinctly-styled link nodes for every
     active `domain_relationships` row. A domain the user has access to and that
     has a live pipeline opens its Dashboard on click; a domain without access
     toasts an explanation and stays on Home; a domain with no pipeline yet shows
     "not yet available." Access-lock takes priority over pipeline-status when
     both apply (verified: `testadmin` has no `product`-domain access row at all,
     so Product renders `locked`, not `planned`).
   - **Browse `<Domain Plural Label>`** (e.g. "Browse Customers") — searchable/
     paginated gold-layer browser for the currently-entered domain, with live
     (debounced) search-as-you-type over every attribute's value (replacing V3's
     hardcoded per-field first_name/last_name/email search), click-through detail
     view with the source crosswalk, inline edit if the user's account has
     `read_write` access to that domain, and an **Audit Trail** button opening a
     read-only history panel. An inline edit submits into the `gold_record_edit`
     maker-checker workflow (1 level, a quorum of 2 *different* Data Owners)
     instead of applying immediately. A golden record currently sitting in a
     pending Match Review pair is locked read-only — an amber "Under review"
     badge, fields disabled regardless of access level, and the update endpoint
     itself 409s if called directly, so the lock holds even if the UI is
     bypassed.
   - **Approvals** — a nav item visible to every signed-in role (not just
     dataSteward/dataOwner), since Admins — who approve Match Review's second
     level and User Administration changes — don't have access to the
     Stewardship Console. The Portal additionally extends `match_review_confirmation`
     visibility to admins specifically (see the domain-agnostic generalization
     section above) — the only place that workflow type is reachable by an
     admin at all. Matches the console's Approvals tab exactly: same split-pane
     layout, category filter/sort, and embedded diff-highlighted record panel.
   - **Audit Trail** — append-only, domain-scoped history of every golden
     record: creation, every edit (manual portal edit, steward real-time
     reprocessing, or batch pipeline recompute), and logical deletes. Opens from
     a button on the record detail modal as a separate panel showing the
     record's key identifiers up top and a newest-first timeline below, grouped
     into one entry per logical change with expandable old→new attribute diffs.
     There is no write/delete endpoint for it by design.
   - **Data Governance** (nav dropdown, four items; the first two are role-gated,
     the last two are visible to every signed-in user since they're read-only
     outside dataSteward/dataOwner):
     - **Data Stewardship** — visible only to `dataSteward`/`dataOwner`. Opens via
       `launchStewardshipApp()`, still `window.open(..., 'mdmStewardshipTab')` (same
       named-tab reuse as before), but now targeting `/app/?domain=<domain_key>` when
       the portal's own `currentDomain` is set, falling back to plain `/app/`
       otherwise — see "Stewardship console navigation & domain-scoping" below.
     - **Lineage and Impact Analysis** — visible with `read`/`read_write` access
       to at least one domain; `/api/v1/lineage/*` stays open to any
       authenticated user regardless of domain access (pipeline metadata, not
       gold data). A full pannable/zoomable **network diagram** of the entire
       real pipeline (`bronze.raw_records` → `stg_source_records` →
       `staged_records` → `records`/`exceptions_queue` → `gold_prep.*` →
       `gold_match_candidates`/`gold_survivorship_winners`/`golden_records`/
       `gold_crosswalk`/`gold_match_review_queue` → `stewardship.*`). **Fixed
       this pass** — see "Lineage tab fix" below for what was stale and how it
       was rebuilt.
     - **Reference Data Maintenance** — Country Codes and State Codes
       (`ref.ref_country_codes`/`ref_state_codes`, global, not domain-scoped —
       universal reference values, not domain-specific business data). Visible
       to everyone; create/edit/deactivate submits into `reference_data_change`
       (1 level, 1 Data Owner).
     - **Rules Configuration** — Column Rules, Matching Rules (plus their tier
       definitions), and Survivorship Rules, now `domain_key`-scoped
       (`attribute_key` instead of a fixed column name) — `GET
       /api/v1/rules-config/{entity_type}` accepts an optional `?domain_key=`
       filter. Submissions go into `rules_config_change` (1 level, quorum of 2
       *different* Data Owners). Both screens (tabs, columns, forms) and both
       apps' Approvals diff-views render entirely from
       `governance.entity_type_registry` (`GET /api/v1/governed-entities`)
       rather than hardcoded per-entity-type config.
   - **Administration → User Administration** — admin-only. Create/edit/
     deactivate users, assign role and, per active domain, a
     `read_write`/`read`/`none` access level (a grid, not a single dropdown, per
     the domain-agnostic generalization). The seed admin account is `mdm_admin`,
     created via `scripts/create_admin_user.py`. Creating a user, or changing
     `role`/domain access/`is_active` on an existing one, submits into
     `user_admin_change` (1 level, 1 Admin approver who must be a different
     admin than the requester) — the new user's per-domain access grants are
     written into `user_domain_access` at approval time
     (`_execute_user_create`/`update_user_domain_access` in `api/main.py`),
     alongside the legacy `gold_access` column. `full_name`-only edits and
     password resets are not gated and still apply immediately.

3. **Maker-Checker Approval Workflow Engine** (`api/workflow_engine.py`, `governance`
   schema) — generic across all three apps and every domain, added so a maker can never be
   the sole authority on a state-changing action. A `workflow_type` is an ordered list of
   steps (`governance.workflow_definitions`/`workflow_steps`); each step names a
   `required_role` and an `approvals_required` count (1 for a simple single approver, >1
   for a same-level quorum of *different* people). The engine itself enforces, independent
   of any caller: a maker can never decide on their own submission, the same approver can
   never cast two decisions on one instance, and rejection at any step is terminal. Six
   workflow types are configured today (confirmed live against `governance.workflow_definitions`):
   `stewardship_remediation` (1 step, `dataOwner`×1), `gold_record_edit` (1 step,
   `dataOwner`, `approvals_required=2` — a quorum of 2 different Data Owners),
   `match_review_confirmation` (2 sequential steps: `dataOwner`×1, then `admin`×1),
   `user_admin_change` (1 step, `admin`×1 — covers create, update, and
   `domain_access_update` action types), `reference_data_change` (1 step,
   `dataOwner`×1), and `rules_config_change` (1 step, `dataOwner`, `approvals_required=2`).
   Generic endpoints — `GET /api/v1/workflows/pending`, `GET /api/v1/workflows/mine`,
   `GET /api/v1/workflows/{instance_id}`, `POST /api/v1/workflows/{instance_id}/decide` —
   serve an **Approvals** view in both the Stewardship Console and the Portal.
   `scripts/create_demo_governance_users.py` seeds the extra `dataOwner`/`admin` accounts
   (`mdm_dataowner2`, `mdm_dataowner3`, `mdm_admin2`) needed to actually clear a quorum or a
   "different approver" requirement in a fresh install, which otherwise ships with only one
   account per role.

4. **Pagination** — every list-backed `GET` endpoint takes `limit`/`offset` (default
   `limit=10`) and echoes `total`/`limit`/`offset` back alongside the page. On the
   frontend, every list view in both apps shares one small pagination module: Prev/Next, a
   10/25/50/100 page-size `<select>`, and a `total` count, with the chosen page size
   remembered in `localStorage` per view key. If a filter, tab switch, or search shrinks the
   result set out from under the current page, the pager re-fetches page 1 of the corrected
   range automatically rather than rendering a blank list.

5. **REST API** — exposes the gold layer, crosswalk, lineage graph, and stewardship queue
   for every active domain, for downstream consumption. Auto-docs at `/docs`.

## Lineage tab fix (this pass)

`dbt_project/seeds/lineage_edges.csv` was still the V3-era 27-row CSV: stale table
names throughout (`bronze.crm_customers_raw`/`erp_customers_raw`, `silver.silver_customers`,
`gold.gold_customers`, `stewardship_app.*`), and it collapsed the real 3-stage silver
pipeline (`stg_source_records` → `staged_records` → `records`/`exceptions_queue`) and
the real `gold_survivorship_winners` intermediate model into single monolithic nodes
that no longer existed. Rewritten to a 45-row CSV against the real V4 pipeline:
- Bronze raw fields are represented as one node per **source system** (`CRM`/`ERP`),
  not one per raw column — `bronze.raw_records` is a single shared table
  differentiated by `source_system`, so a per-raw-column node (22 of them) would have
  bloated the graph without adding real traceability; the per-field mapping detail
  (real `governance.source_field_mappings` IDs) lives in each edge's own description
  instead.
- The real intermediate silver stages (`stg_source_records`, `staged_records`,
  `records`, `exceptions_queue`) and the real `gold_survivorship_winners` node are
  all present as distinct nodes with real edges between them, using real
  `source_field_mappings`/`matching_rules`/`column_rules` IDs where one exists.
- `portal_app/frontend/index.html`'s `NODE_DESCRIPTIONS` dict was rewritten to
  match, and the golden-record lookup placeholder updated to the real
  domain-qualified format (`GOLD-CUSTOMER-00001`, not `GOLD-00001`).
- `layoutGraph()` was changed to lay out graph columns by **layer+table**, not bare
  layer: V4's real pipeline has several distinct tables per conceptual layer (four
  in silver alone), so the old bare-layer grouping stacked dozens of nodes into one
  unreadable column once the CSV became accurate. Each real table now gets its own
  column, ordered by (LAYER_RANK, table name) so layers still read left-to-right in
  pipeline order.

Verified live end-to-end with Playwright against a running API + portal (not just
code review, per this project's own testing convention): graph renders (41 nodes),
no stale terms present anywhere on the page, node detail panel loads correctly for
both a bronze and a gold node, no console errors.

## Product domain onboarding (this pass)

Product's schema, sources, and rule configuration were metadata-only from the initial
V4 generalization pass (see above) until this pass, which gave it the same kind of
real, tested pipeline Customer has always had — added the same way the generalization
work was designed to allow: additive metadata rows plus a small number of genuinely
new, reusable rule-type/transform-function branches in already-generic code, not a
rewrite of any existing dbt model or Python file. Exactly two new rule types were
needed (`normalize_code` for matching, `source_priority` for survivorship — both
below), one new file (the Product source-data generator), and one incidental bug fix
in unrelated pre-existing code (also below).

**Three source systems** (`governance.domain_source_systems`, domain_key='product'):
**ERP** (commercial + operational attributes — price, status, supplier linkage),
**PLM** (engineering attributes — spec versions, physical properties), and
**SUPPLIER** (the supplier's own spec sheet — lead time, minimum order quantity,
their own part number). Three sources instead of Customer's two, chosen specifically
so the platform's attribute-mapping story has a case with more than one
non-authoritative source per attribute to reconcile, and so bronze proves out a
second file format: ERP and PLM land as CSV, exactly like Customer's CRM/ERP, but
SUPPLIER lands as an **XLSX** spreadsheet (a supplier's own hand-maintained catalog
is realistically not going to arrive as a clean CSV export). `scripts/load_bronze.py`
gained a `file_type: 'xlsx'` option in its per-source `SOURCES` config — read via
`pandas.read_excel(dtype=str)`, then registered as a DuckDB view via
`con.register()` — every other line of `load_bronze.py`'s logic (watermark
filtering, batch logging, full-vs-incremental detection) is unchanged and
file-type-agnostic; xlsx support was additive configuration, not a new code path.

**Schema** (`governance.domain_attributes`, 20 rows, sort_order 1-20): `product_name`
(required, `is_similarity_text_field`), `manufacturer_part_number` (required,
`is_matching_field` — the tier-1 exact-match key), `manufacturer_name`
(`is_similarity_text_field`), `category` (required, `is_blocking_key`,
`is_dashboard_breakdown`), `unit_of_measure`, `list_price`, `currency_code`,
`product_status` (required, `is_dashboard_breakdown`), `lifecycle_status`,
`weight_kg`, `material`, `color`, `country_of_origin`, `supplier_id` (required),
`supplier_part_number`, `lead_time_days`, `minimum_order_qty`,
`engineering_spec_version`, `source_created_date`, `source_modified_date`.

**Source field mappings are deliberately asymmetric**
(`governance.source_field_mappings`, SFM100-137) — this is the actual point of
having three sources, not an oversight: only `product_name`/
`manufacturer_part_number`/`category` are mapped by all three sources (they have to
be, for matching and blocking to work regardless of which sources a given product
happens to be in); `manufacturer_name`/`material`/`color`/`country_of_origin` come
from PLM+SUPPLIER only (not ERP); `unit_of_measure`/`weight_kg` come from ERP+PLM
only (not SUPPLIER); `list_price`/`currency_code`/`product_status` are ERP-only;
`lifecycle_status`/`engineering_spec_version` are PLM-only; `supplier_id`/
`supplier_part_number` are ERP+SUPPLIER only (not PLM); `lead_time_days`/
`minimum_order_qty` are SUPPLIER-only — mirroring how a real
commercial/engineering/supplier-catalog split actually works. A new
`transform_function`, `numeric_extract`, was added to
`dbt_project/macros/transform_expr.sql` for this: ERP's `gross_weight` field arrives
as free text like `"4.20 kg"`, and `numeric_extract` pulls the first numeric token
out of it (PLM's own `weight_kg` column is already clean, so its own mapping just
uses `trim`) — the same "handler registry, add a branch" pattern every other
`transform_function` already followed.

**Validation** (`bus_rules.column_rules`, R100-R114, 15 rules across all three
sources): `not_null` on `product_name`/`manufacturer_part_number`/`supplier_id`/
`vendor_account_id` per source; `regex` on ERP's `unit_price`
(`^\d+(\.\d{1,2})?$`), PLM's `weight_kg` (`^\d+(\.\d+)?$`), and SUPPLIER's lead-time
and MOQ fields (`^\d+$`); `reference_country` on PLM's `origin_country` and
SUPPLIER's `country_of_mfg`; `trim_case_proper` corrections on ERP's `item_status`
and PLM's `mfr_name`. No new `rule_type` was needed here — all 15 rules reuse the
existing `not_null`/`regex`/`reference_country`/`trim_case_proper` vocabulary
`api/validation.py` and `dbt_project/macros/rule_type_expr.sql` already implement.

**Matching** (`bus_rules.matching_rules` MR100-103, `matching_thresholds` MT100-102):
tier 1 is exact match on `manufacturer_part_number`, using a genuinely new
`transform_function`, `normalize_code` (strips non-alphanumerics, uppercases —
added to `scripts/generate_matches.py`'s `TRANSFORM_FUNCTIONS` and
`api/reprocessing.py`'s `PY_TRANSFORMS`/`SQL_TRANSFORMS`), so `"MPN-4471-A"` and
`"mpn4471a"` compare equal even though no two sources format their part numbers the
same way. Tier 2 is the same fuzzy engine Customer uses (TF-IDF character n-gram
cosine similarity), over `product_name` + `manufacturer_name` text, blocked by
`category`. MT102's `review_lower_threshold` is **0.60** — notably higher than
Customer's 0.35 — a deliberate calibration, not a copy-paste value: with only a
handful of distinct product component types per category, unrelated products that
merely share a category and a common component word (e.g. two different "bearing"
products) can otherwise cross a low bar purely on shared vocabulary, flooding the
Match Review queue with non-matches. 0.60 was tuned empirically against this
domain's own generated test data to keep that queue to genuinely plausible
candidates only.

**Survivorship** (`bus_rules.survivorship_rules` SR100-117, one rule per
non-structural attribute): introduces a genuinely new `rule_type`,
`source_priority` — added to `dbt_project/models/gold/gold_survivorship_winners.sql`
and `api/reprocessing.py`'s `SURVIVORSHIP_RANK_HANDLERS` — which prefers whichever
contributing record's `source_system` equals the rule's `rule_param` outright,
regardless of recency: the platform's answer to "always take this attribute from
ERP if ERP is present," a real MDM survivorship pattern the existing
`newest`/`oldest`/`pattern_match`/`most_common`/`most_complete`/`shortest_value`
vocabulary had no way to express. Product uses every `rule_type` the platform
supports: `newest` (`product_name`), `oldest` (`category`), `pattern_match`
(`engineering_spec_version`, preferring the `"REV-<major>[.<minor>]"` format),
`shortest_value` (`color`), `most_common` (`unit_of_measure`,
`country_of_origin`), `most_complete` (`manufacturer_name`, `weight_kg`,
`material`, `supplier_id`, `supplier_part_number`), and `source_priority`
(`manufacturer_part_number`/`lifecycle_status` → PLM; `list_price`/
`currency_code`/`product_status` → ERP; `lead_time_days`/`minimum_order_qty` →
SUPPLIER). The `rule_type` choice per attribute is not arbitrary: `newest`/`oldest`
rank purely by date, blind to whether a given contributing record's value for that
attribute is actually populated, so they're only safe for the three attributes
every source maps (`product_name`, `manufacturer_part_number`, `category`) — using
either on an asymmetrically-mapped attribute could in principle pick a source's
blank/null value over another source's populated one, just because it happens to be
more recently modified. Every asymmetrically-mapped attribute instead uses
`most_complete` (safe regardless of which source combination contributed to a given
match group) or `source_priority` naming that attribute's one sole mapping source
(also always safe by construction) — verified directly (see below), not just
reasoned about.

**Test data generator** (`data/generate_product_source_data.py`, new file,
mirroring `data/generate_source_data.py`'s conventions — deterministic,
`random.seed`/`Faker.seed` fixed): generates ~145 synthetic master products across
17 sub-categories (`FAST1-3`, `BRNG1-2`, `ELEC1-2`, `HYDR1-2`, `SENS1-2`, `PACK1-2`,
`SAFE1-2`, `PNEU1-2`). Every product name carries a unique `#NNNN` tag — a
realistic industrial-catalog convention, and also deliberate insurance against
tier-2 fuzzy matching scoring unrelated same-category products as falsely similar.
Each master product is assigned one of eight patterns controlling which of the
three sources carry it and how well their data agrees: `ALL3_EXACT` (in all three,
part number reformatted differently per source but normalizing identically — the
"slightly mismatched across sources" case), `ALL3_FUZZY_CONFIDENT` (in all three,
part number genuinely differs so tier 1 misses it, but `product_name` is close
enough for tier 2 to auto-merge), `ALL3_FUZZY_SUSPECT` (same, but `product_name` is
more loosely worded — lands in Match Review), `ERP_PLM` / `SUPPLIER_PLM` (pairs,
missing the third source — the "in the supplier catalog but not yet in ERP" case,
and its converse), and `SUPPLIER_ONLY` / `ERP_ONLY` / `PLM_ONLY` (singletons).
Deliberately invalid rows are injected only into each source's own `*_ONLY` pool, so
they never disturb the matching-calibration rows, hitting each of the 15
`column_rules`. Verified row counts: ERP=85, PLM=90, Supplier=87 (all in the
80-100-row-per-source range Customer's own generator targets). **Does not support
incremental mode** — no `--incremental` flag, unlike `generate_source_data.py` —
deferred deliberately to later work.

**End-to-end verification** (full `scripts/build_pipeline.py` run in a sandbox
clone, Customer and Product together, no regressions): Product yields 130 golden
records from 262 total source rows — 61 singletons, 69 confident auto-merged
multi-source golden records (spanning both the tier-1 exact and tier-2 auto-merge
paths), 28 pairs sitting in the Match Review queue awaiting steward action, and 27
rows rejected to the exceptions queue for failing validation (9 ERP, 8 PLM, 10
Supplier). A dedicated check confirmed zero cases where a survivorship rule picked
a blank/null winning value when a populated alternative existed in the same match
group — every `source_priority` pick was independently verified to come from the
intended preferred source whenever that source was present in the group, closing
the null-safety design concern above with a real check, not just the design
argument.

**One bug fixed in existing (pre-Product) code while onboarding**:
`scripts/migrate_column_rules_to_domain.py`'s `migrate()` used to reassign
`domain_key='customer'` to every row in `bus_rules.column_rules` unconditionally on
every run, with no `WHERE domain_key IS NULL` guard — harmless while Customer was
the only domain with column rules, but would have silently stomped Product's
`domain_key='product'` rows back to `'customer'` on the very next
`scripts/build_pipeline.py` run once Product went live. Fixed by adding the same
`WHERE domain_key IS NULL` guard `api/db.py`'s equivalent in-code migration already
had — closing a pre-existing drift between the two migration paths that happened to
be invisible until a second domain's rows existed to stomp.

`requirements.txt` gained `openpyxl` (pandas' xlsx engine), needed for the Supplier
source.

**Not yet done for Product**: incremental loads.
`data/generate_product_source_data.py` has no `--incremental`/`--full` mode, and
`load_bronze.py`'s incremental auto-detection, while domain-agnostic in principle,
has only ever been exercised against Customer's generator output — Product's
`bronze.load_control` rows exist and a full load works, but the incremental path
for this domain is unverified and explicitly deferred, not silently assumed to
work.

## Stewardship console navigation & domain-scoping (this pass)

Until this pass, the Stewardship console (`stewardship_app/frontend/index.html`) was
deliberately domain-agnostic/cross-domain — every tab mixed every domain a steward
could see, with no "current domain" concept at all (each record just carried its own
`domain_key` for per-record attribute lookups, same as it always has). This pass gave
it a real per-session "selected domain" concept, mirroring the portal's
`currentDomain`, and wired up navigation between the two apps. Four pieces:

**1. Domain carries over from portal to Stewardship.** The portal's
`launchStewardshipApp()` (previously just `window.open('/app/', 'mdmStewardshipTab')`)
now opens `/app/?domain=<domain_key>` when the portal's own `currentDomain` is set,
falling back to plain `/app/` when it isn't (tab-name reuse via `'mdmStewardshipTab'`
is unchanged). Stewardship reads `?domain=` off `window.location.search` in a new
`determineInitialView()` function, called from `enterApp()` right after
`loadDomainsList()`. If the named domain exists, the user `has_access`, and it has a
live pipeline (`pipeline_status === 'live'`) — the same gates the portal itself
applies in `enterDomain()` — it sets Stewardship's own new `stewardshipDomain` global
and jumps straight to that domain's Dashboard. Otherwise it falls through to (2).

**2. Stewardship's own Home diagram, for direct logins.** Opening Stewardship with no
valid `?domain=` now lands on a brand-new "Home" tab: a pannable/zoomable
domain-relationship diagram ported essentially verbatim from the portal's own Home
tab — same SVG pan/zoom mechanics (drag to pan, wheel to zoom, fit-to-view on first
render) and the same node/edge visual language (domain nodes color-coded
live-access/planned/locked, diamond-shaped relationship link nodes). New CSS
(`.home-node-box`, `.home-edge-line`, `.home-link-node`, `.home-lock-icon`,
`.home-intro`, `.home-legend`, `.legend-dot`, `#homeCanvasWrap`, plus a new
`--blue-dim` CSS variable) and a new `escapeXml()` helper (distinct from
Stewardship's pre-existing `escapeApprovalText()`, which serves a different escaping
context) were added. Clicking a domain node calls a new `enterStewardshipDomain(domainKey)`
function — mirrors the portal's `enterDomain()`: same access/pipeline-status gating,
same toast messages on a locked or not-yet-available domain — which sets
`stewardshipDomain` and switches to that domain's Dashboard. A persistent "Home" tab
button was also added to the Stewardship nav bar (not explicitly required, but a
low-risk way to let a steward switch domains later without logging out —
`switchView('home')` is always reachable regardless of whether a domain is currently
selected).

**3. Domain name in the header; all four tabs genuinely scoped to it.** The header's
`<h1>Data Stewardship Console</h1>` now has a `<span id="headerDomainSuffix">`
appended, updated by a new `updateHeaderDomainSuffix()` function to read e.g. "
(Product)" once `stewardshipDomain` is set — so the header reads "Data Stewardship
Console (Product)". Per tab:
   - **Exception Queue / Match Review** — `GET /stewardship/queue` and
     `GET /stewardship/match-review` already accepted an optional `domain_key` query
     param server-side (pre-existing, previously used only by the portal's own
     per-domain drill-throughs, unused by this console). The console's
     `buildQueueParams()` helper (the single choke point both queue-loading
     functions build their query params through) now appends
     `domain_key=stewardshipDomain.domain_key` whenever it's set.
   - **Dashboard** — `GET /stewardship/dashboard/exceptions-summary` and
     `GET /stewardship/dashboard/match-review-summary` also already accepted an
     optional `domain_key` (pre-existing). `loadDashboard()` now appends it to both
     fetch URLs. The one part of those two summaries that was NOT already
     domain-aware was the "Your Activity" widget: its backend helper
     `_my_workflow_activity(workflow_type, user_id)` in `api/main.py` counted a
     user's own workflow submissions across ALL domains regardless of the
     `domain_key` filter applied to the rest of the summary. Fixed by adding an
     optional third parameter, `domain_key=None`: when set, for
     `workflow_type='stewardship_remediation'` it filters to `entity_id IN (SELECT
     exception_id FROM main_silver.exceptions_queue WHERE domain_key = ?)`, and for
     `'match_review_confirmation'` to `entity_id IN (SELECT pair_id FROM
     main_gold.gold_match_review_queue WHERE domain_key = ?)` — because
     `governance.workflow_instances` itself has no `domain_key` column; a workflow
     instance's domain is only knowable by joining out to whichever queue table owns
     its `entity_id`. Both call sites in `api/main.py` (the two dashboard summary
     endpoints) now pass `domain_key` through. Verified directly against the
     project's DuckDB file that both queue tables have real `domain_key`-tagged rows
     for both Customer and Product (Product: 27 exceptions / 28 match-review
     candidates; Customer: 16 / 2) and that the new SQL shape runs cleanly.
   - **Approvals** — this one had no existing `domain_key` filter to reuse.
     `GET /workflows/pending` and `GET /workflows/mine` return workflow instances of
     ANY workflow type across the whole platform, and — critically — not every
     workflow payload carries a `domain_key` (a `match_review_confirmation`
     instance's payload is just `{steward_note}`; only some `stewardship_remediation`
     instances' payloads happen to include one, depending on `action_type`). Rather
     than touch that shared endpoint (used by both apps, for six workflow types
     total) or add a new column to `governance.workflow_instances` (out of scope for
     this pass), domain-scoping was done client-side in the Stewardship console only:
     a new `domainScopedEntityIdSets(domainKey)` function fetches the full
     (`status=all`) exception-id set and pair-id set for the selected domain from the
     two already-domain-filterable queue endpoints, and `loadApprovalsQueue()` now
     filters its (pre-existing) type-restricted instance list — Stewardship's
     Approvals tab only ever originates `stewardship_remediation`/
     `match_review_confirmation` instances, via the pre-existing
     `STEWARDSHIP_WORKFLOW_TYPES` constant — down to just the instances whose
     `entity_id` is in the matching set.
   - A guard was added to `switchView()`: if the requested view isn't `'home'` and
     `stewardshipDomain` isn't set, it falls back to `'home'` — mirroring the
     portal's own pre-existing Dashboard/Browse-to-Home guard — so none of the four
     scoped tabs can ever render with nothing to scope to.
   - `doLogout()` now resets `stewardshipDomain = null` (an analogous latent gap was
     also fixed in the portal's own `doLogout()`, resetting `currentDomain = null`) so
     a second login in the same browser tab can't silently inherit the previous
     user's domain selection.

**4. Portal Home page: Dashboard/Browse/Approvals nav disabled with no domain
selected.** A new `updateNavDomainGating()` function in
`portal_app/frontend/index.html` sets `.disabled = true` and
`title = 'Please select a domain'` on the `navDashboard`/`navBrowse`/`navApprovals`
nav buttons whenever `currentDomain` is null, clearing both once a domain is entered.
Called from `enterApp()` (initial post-login state) and from `enterDomain()` (right
after `currentDomain` is set). New CSS: `.view-tab:disabled { opacity: 0.4; cursor:
not-allowed; }`. The native `title` attribute is what shows "Please select a domain"
as a hover tooltip — no custom tooltip component needed. This only gates the *nav
buttons themselves*; it does not change what the portal's own Approvals tab shows
(that tab was and remains cross-domain in the portal, unlike Stewardship's now-scoped
one — out of scope for this ask, which was specifically about disabling the menu
options, not further scoping their content).

Files touched: `portal_app/frontend/index.html`, `stewardship_app/frontend/index.html`,
`api/main.py` (only the one `_my_workflow_activity` fix — no other backend changes).
No new Python dependencies, no database schema changes, no dbt model changes.
Verified: both frontend files' inline `<script>` blocks pass `node --check`;
`api/main.py` passes `ast.parse`; the new SQL shape was smoke-tested directly against
the project's own `mdm_demo.duckdb`; all new/changed HTML element IDs referenced from
JS were cross-checked against the HTML for existence, each container ID confirmed to
appear exactly once.

## Key data model notes

- `governance.domain_registry` / `domain_attributes` / `domain_source_systems` /
  `source_field_mappings` / `domain_relationships` / `user_domain_access` — see
  "V4: domain-agnostic generalization" above for the full shape and rationale.
- `bus_rules.column_rules` — the single source of truth for cleansing/standardization/
  validation rules, now `(domain_key, attribute_key)`-scoped, referenced by rule ID
  (R001, R002...) throughout the pipeline, the exception queue's reject_reasons, and the
  stewardship UI. DB-native (schema `bus_rules`), maintained via the Rules Configuration
  screen's maker-checker workflow.
- `dbt_project/seeds/lineage_edges.csv` — column-level lineage metadata (from_layer/table/
  column → to_layer/table/column, transform_rule_id, description). Powers both the
  Data Governance network diagram and the `/api/v1/lineage/*` endpoints. Still a dbt seed.
  Rewritten this pass — see "Lineage tab fix" above. Note: wildcard nodes (e.g.
  `gold.gold_match_candidates.*`) must be explicitly linked to specific columns or
  impact-analysis chains silently break — re-verify with `/api/v1/lineage/impact` and
  `/trace` after touching this file, don't just eyeball it.
- `bus_rules.matching_thresholds` — one row per (domain_key, matching tier) (tier_id,
  tier_order, tier_name, match_method, is_match_tier, auto_merge_threshold,
  review_lower_threshold, active, description). DB-native, maintained via Rules
  Configuration's "Matching Tiers" tab. `match_method` is `'exact'` or
  `'fuzzy_tfidf_cosine'` today; a non-tier row (`is_match_tier=false`,
  `match_method='no_match_baseline'`) holds the 0.50 provisional-confidence value.
  Adding a tier means adding a row here (plus its rules below) — nothing in
  `scripts/generate_matches.py` or `api/reprocessing.py` needs to change to add another
  exact or fuzzy tier, only to add a genuinely new match_method.
- `bus_rules.matching_rules` — child table keyed by `(domain_key, tier_id)`, one row per
  attribute a tier operates on (rule_role is `exact_match_field`, `similarity_text_field`,
  or `blocking_key`; `transform_function` is `none`/`normalize_email`/`normalize_phone`/
  `normalize_code` (the last added for Product, see "Product domain onboarding" above),
  applied identically in both Python (`scripts/generate_matches.py`, `api/reprocessing.py`)
  and SQL via matching name-keyed registries). Multiple `blocking_key` rows for one tier
  compose into a multi-attribute blocking key; `rule_order` controls concatenation order for
  `similarity_text_field` rows. Today's Customer config: tier 1 exact on email/phone; tier 2
  fuzzy similarity on first_name/last_name/address_line1/address_line2/city, blocked on
  state_code. Product's config: tier 1 exact on manufacturer_part_number (via
  `normalize_code`); tier 2 fuzzy similarity on product_name/manufacturer_name, blocked on
  category.
- `bus_rules.survivorship_rules` — one active rule per (domain_key, attribute_key),
  driving attribute-level survivorship. `rule_type` is `most_common`, `most_complete`,
  `oldest`/`newest`, `pattern_match`, `shortest_value`, or `source_priority` (the last
  added for Product — prefers a named source outright regardless of recency; see
  "Product domain onboarding" above for the null-safety rationale behind which
  rule_type suits which kind of attribute). Exactly one primary rule per
  attribute; whenever it doesn't produce a clean winner, `source_modified_date` desc (then
  `domain_source_systems.sort_order`) is the universal, non-configurable tie-break.
  Defaults are all `newest`, which reproduces record-level behavior until a governance user
  tunes a specific attribute. Evaluated by
  `dbt_project/models/gold/gold_survivorship_winners.sql` (batch) and
  `api/reprocessing.py`'s real-time equivalent — the two must stay in lockstep.
- `ref.ref_country_codes` / `ref.ref_state_codes` — DB-native reference data (schema `ref`),
  global (not domain-scoped), maintained via the Reference Data Maintenance screen's
  maker-checker workflow. Each has a human-readable label alongside the code, plus
  `is_active`. `api/validation.py` queries these live (no caching).
- `gold_prep.match_groups` / `match_edges` / `match_review_candidates` — written by
  `scripts/generate_matches.py` (not dbt-built), domain_key-scoped, same "Python loads, dbt
  treats it as a source" pattern as bronze. `match_edges` is the audit trail dbt joins to
  compute `gold_crosswalk.match_confidence_score`.
- `auth.users` / `auth.sessions` — bcrypt password hashes, bearer tokens, 8hr TTL.
  `auth.users.gold_access` is the legacy single global access column — see "V4:
  domain-agnostic generalization" above for why it's no longer the real access-control
  source of truth.
- `stewardship.remediation_log` / `remediated_records` / `exception_status_overrides` —
  audit trail and status tracking layered on top of the dbt-built `exceptions_queue` table
  (itself a static point-in-time snapshot; live status lives in the overrides table).
  **Renamed from `stewardship_app` this generalization** — see above.
- `stewardship.match_review_overrides` / `match_review_log` — same live-status-overlay
  pattern, for `gold_match_review_queue` instead of `exceptions_queue`. A 'confirmed'
  override is realized in real time by `api/reprocessing.py`'s
  `reprocess_confirmed_match_review` the moment the maker-checker chain approves it;
  `generate_matches.py`'s forced union-find merge on the next batch run is a fallback for
  the rare case that real-time step itself failed. A 'rejected' override permanently
  excludes that pair. 'cancelled' is a system-written terminal status distinct from
  'rejected' — see "Match Review hold" above.
- `audit.audit_trail` — append-only, domain-scoped gold-layer audit trail (`api/audit.py`).
  One row per changed attribute per logical operation, grouped by `change_batch_id`. Three
  writers: `api/main.py`'s `update_record()` (`event_source='portal_manual_edit'`),
  `api/reprocessing.py` (`'steward_reprocessing'`), and `scripts/audit_pipeline_diff.py`
  (`'pipeline_batch'`, run as the final step of `scripts/build_pipeline.py`). No code
  anywhere issues an UPDATE/DELETE against this table.
- `governance.workflow_definitions` / `workflow_steps` / `workflow_instances` /
  `workflow_decisions` — the maker-checker engine's own tables (`api/workflow_engine.py`).
  Instances carry a JSON `payload` (whatever the completion callback needs to apply the
  change) and, once approved/rejected, a JSON `result`; decisions are one row per approver
  per step, which is how the engine enforces "no double-deciding" and quorum counts.
- `audit.golden_records_snapshot` — a copy of `main_gold.golden_records`'s tracked
  attributes as of the last batch pipeline run, kept in the `audit` schema specifically
  because `dbt run` never touches that schema (it CREATE OR REPLACEs
  `main_gold.golden_records` every run). `scripts/audit_pipeline_diff.py` diffs the fresh
  gold table against this snapshot to detect batch-driven creates/updates/logical-deletes,
  then refreshes it.

## Known simplifications (documented on purpose, not oversights)

- **Product now also has a real pipeline** (see "Product domain onboarding" above) —
  it was metadata-only (`domain_registry`/`domain_relationships` rows, no
  attributes, no source systems, no gold data, every domain-scoped endpoint 409ing
  it via `_require_live_domain`) for most of this project's life, kept that way
  specifically so the Home diagram, domain locking, and "not yet available"
  handling had a second real domain to demonstrate against without needing a second
  full dataset. That demonstration purpose is now historical: Product's
  `domain_registry.pipeline_status` is `live`, same as Customer's, and every
  domain-scoped endpoint serves it normally. One gap remains open by explicit
  choice: **Product's incremental load path is not yet implemented** — see "Product
  domain onboarding" above.
- The AI remediation heuristic fallback (`api/ai_remediation.py`) only generalizes for
  the Customer domain (`HEURISTIC_FALLBACK_DOMAINS`) — its email/state/country
  pattern-matching heuristics are meaningful for that field set specifically. Every
  other domain gets a generic "no heuristic available, needs manual review" response
  from the fallback path; only the AI path (when `ANTHROPIC_API_KEY` is set) works
  generically for any domain.
- Fuzzy matching (the TF-IDF similarity scoring itself, i.e. finding NEW borderline
  candidates) only runs in the batch pipeline (`scripts/generate_matches.py`).
  Real-time reprocessing (steward resolves an exception → immediate re-match) still only
  evaluates the `match_method='exact'` tier -- fitting a TF-IDF vectorizer per API request
  was judged not worth the latency for a demo-scoped feature. Both paths read the same
  `exact_match_field` rules from `bus_rules.matching_rules`, so they can't drift on *which*
  attributes count as an exact match, even though only the batch step can also run a fuzzy
  tier. This is unrelated to *realizing* an already-decided Match Review confirmation,
  which is a merge/survivorship operation on a known pair — that happens in real time.
- The fuzzy tier is TF-IDF character n-gram cosine similarity, not a neural sentence
  embedding -- a deliberate lightweight choice (see Tech stack); the vectorizer's shape
  (char_wb analyzer, 2-4 char n-grams) is a fixed code constant in `generate_matches.py`,
  not rule metadata. Thresholds (0.80 auto-merge, 0.35 review floor today, for Customer)
  live in `bus_rules.matching_thresholds`'s `auto_merge_threshold`/`review_lower_threshold`
  columns for the fuzzy tier row, not hardcoded constants. Original calibration was
  empirical, against this project's synthetic data generator's 12 seeded fuzzy-duplicate
  pairs vs. every same-state non-duplicate pair; re-calibrate if the generator's population
  changes materially.
- Attribute-level survivorship's `most_common`/`most_complete`/`pattern_match` rule types
  are evaluated independently per attribute across whatever sources contributed to that
  match group -- there's no cross-attribute consistency check (e.g. a golden record could
  in theory draw `city` from one source and `state_code` from another even if that pairing
  wouldn't occur together in any single real source record). Accepted for a demo-scoped
  feature; a production system would likely want a "prefer values from the same winning
  source when reasonably possible" refinement.
- Steward corrections ARE re-validated against the reject-severity metadata rules
  (`api/validation.py`, mirroring `bus_rules.column_rules`) before reprocessing runs — a
  correction that still fails validation is rejected with an alert and the record
  stays in the queue (status remains `open`). Only a genuinely valid correction is
  treated as authoritative and flows into match/merge.
- Real-time reprocessing's match step compares against each golden record's *current*
  survivor values, not every historical non-survivor source in that group.
- New golden IDs from real-time reprocessing increment the current max within that
  domain, which can diverge from what a full `dbt run` would assign from scratch. The
  audit trail inherits this: if a reprocessing-created golden_id gets renumbered by the
  next full rebuild, `audit_pipeline_diff.py` logs the old number as `logically_deleted`
  and the new number as `created`, even though it's the same underlying record — not a
  bug in the diff logic, an accepted consequence of the numbering scheme itself.
- Incremental silver staging (`stg_source_records.sql`, dbt `incremental` materialization)
  only avoids rescanning bronze's full history — it deliberately does NOT extend to
  `staged_records.sql`/`records.sql`/`exceptions_queue.sql` or any gold model, which all
  stay full-`table` rebuilds every run. That's what makes deletion-propagation and
  match-group recompute work with zero special-case code, but it also means gold's own
  rebuild cost, and `scripts/generate_matches.py`'s fuzzy-tier matching cost, are both
  still proportional to total data size per domain, not to what changed. Fine at this
  project's volumes; not a solution to the enterprise-scale gap noted below.
- **Enterprise-scale limitation, not addressed by the incremental staging work above, and
  not addressed by domain-agnostic generalization either**: at real enterprise volumes
  (tens to hundreds of millions of records, or dozens of live domains), two separate
  things break. (1) The fuzzy-matching tier's blocking step does an exhaustive pairwise
  comparison within each block (`scripts/generate_matches.py`) — a block of even a few
  million records makes that comparison count infeasible in a single-node Python process;
  a production system would need approximate similarity search (LSH, a vector index) or a
  dedicated entity-resolution engine (Zingg, Senzing, AWS Entity Resolution, a commercial
  MDM platform's matching layer). (2) Gold (and everything downstream of staging) is
  deliberately rebuilt from scratch every run, per domain — correct and cheap at ~200 rows,
  but proportional to total data size at scale. A production system would need genuinely
  incremental, affected-group-only recompute in gold, on top of replacing the single-file
  DuckDB store with a real warehouse/lakehouse. Documented explicitly as a gap in
  `Enterprise_Readiness_Assessment.docx`.
- The audit trail only captures a manual `dbt run --select gold.*` if
  `scripts/audit_pipeline_diff.py` is run immediately afterward (automatic inside
  `build_pipeline.py`, but not if you run the gold rebuild command by itself).
- Auth is sized for a demo: no MFA, no refresh tokens, single local DuckDB file (not a
  concurrent multi-user warehouse).
- If the domain action itself fails after every required approval is already recorded
  (e.g. a downstream write error), the instance is still marked `approved` — the human
  decision stands — with the failure captured in its `result` field rather than silently
  rolled back; there is no automatic retry. A quorum or multi-level chain also needs enough
  distinct people in the required role to ever clear — hence
  `scripts/create_demo_governance_users.py`.

## Repo structure

```
data/                  synthetic source data generators (Faker-based; domain_key is config
                        inside each script, not a CLI arg): generate_source_data.py
                        (Customer domain, CRM+ERP CSV, ~100 rows/source) and
                        generate_product_source_data.py (Product domain, ERP+PLM CSV plus
                        a SUPPLIER xlsx, ~85-90 rows/source; no --incremental mode yet)
scripts/                load_bronze.py (also writes bronze.load_batches, domain_key-scoped),
                        generate_matches.py (fuzzy match/merge, loops over every domain
                        gold_prep has silver data for; also enforces the Match Review hold),
                        cancel_stale_match_reviews.py, build_pipeline.py (runs the 10-stage
                        build: ensure_governed_schemas.py, ensure_domain_metadata.py,
                        migrate_column_rules_to_domain.py, migrate_matching_rules_to_domain.py,
                        dbt seed, dbt run --exclude gold.*, cancel_stale_match_reviews.py,
                        generate_matches.py, dbt run --select gold.*, audit_pipeline_diff.py),
                        ensure_governed_schemas.py (provisions ref.*/bus_rules.*/
                        governance.entity_type_registry before dbt runs),
                        ensure_domain_metadata.py (provisions domain_registry/domain_attributes/
                        domain_source_systems/domain_relationships/user_domain_access;
                        additive, idempotent, includes migrate_user_domain_access()),
                        migrate_column_rules_to_domain.py / migrate_matching_rules_to_domain.py
                        (one-time, idempotent domain_key-scoping migrations),
                        audit_pipeline_diff.py, create_admin_user.py, reset_admin_password.py,
                        create_demo_governance_users.py
dbt_project/            seeds (only lineage_edges.csv -- everything else migrated to DB-native
                        governance/bus_rules/ref schemas). models: silver/
                        (stg_source_records.sql, staged_records.sql, records.sql,
                        exceptions_queue.sql) and gold/ (gold_match_candidates.sql,
                        gold_survivorship_winners.sql, golden_records.sql, gold_crosswalk.sql,
                        gold_match_review_queue.sql); gold reads generate_matches.py's output
                        via the gold_prep source and bus_rules.*/ref.*/governance.* via their
                        own sources. macros/: active_domain_sources.sql (which domain/source
                        combos are active, driving stg_source_records.sql's per-combo loop),
                        mapped_attributes.sql (standardizes raw fields into canonical
                        attribute keys per governance.source_field_mappings),
                        transform_expr.sql (standardization handlers: trim/proper_case/
                        upper_trim/lowercase_trim/phone_e164_us/numeric_extract),
                        validation_block.sql
                        (the current reject-rule generator, reads bus_rules.column_rules),
                        rule_type_expr.sql (column_rules.rule_type handler registry:
                        not_null/regex/reference_state/reference_country), proper_case.sql,
                        latest_load_batch.sql. column_rule_flags.sql is DEAD CODE --
                        superseded by validation_block.sql, unreferenced by any model, kept
                        only for its own docstring's migration history; safe to delete
                        whenever someone gets to it, not yet done.
api/                    FastAPI app: main.py, db.py, auth.py (domain_access_level(),
                        require_domain_read/write), reprocessing.py, lineage.py,
                        ai_remediation.py (domain-aware, Customer-only heuristic fallback),
                        audit.py, workflow_engine.py
stewardship_app/frontend/   exception queue + match review console (plain HTML/JS), metadata-
                        driven off the domain of whichever queue item is open, plus an
                        Approvals tab over the maker-checker workflow engine
portal_app/frontend/        login-gated portal: Home (domain relationship diagram), browse,
                        governance graph, admin, and an Approvals view (plain HTML/JS)
requirements.txt        pinned deps (duckdb==1.5.4 -- NOT the same version number as the
                        dbt-duckdb adapter, a mistake made once already, worth double-checking;
                        also mcp>=1.9,<2.0 and croniter for the MCP agent orchestration layer,
                        and openpyxl for pandas' xlsx read path, needed for Product's
                        SUPPLIER source)
README.md               Windows-first setup instructions (PowerShell), plus macOS/Linux
```

## Outstanding / not yet built

- Attribute-level survivorship's cross-attribute consistency (see Known simplifications) is
  an accepted gap, not a planned build item.
- `dbt_project/macros/column_rule_flags.sql` is confirmed dead code (superseded by
  `validation_block.sql`) — not yet deleted, just documented as such.
- **Reference Data Maintenance / Rules Configuration extensibility** — the Phase 0/1/2
  work described here predates the V4 domain-agnostic generalization and is unrelated to
  it; still accurate and still done:
  - *Done — Phase 0 (fail loudly).* An unrecognized `survivorship_rules.rule_type` or
    `matching_thresholds.match_method` raises loudly (DuckDB `error(...)` / `RuntimeError`)
    instead of silently falling through; `api/main.py`'s `submit_rules_config_change`
    rejects (400) any `rule_type`/`match_method`/`transform_function` value with no real
    handler at submission time.
  - *Done — Phase 1 (metadata-driven admin/CRUD layer).* `governance.entity_type_registry`
    (table name, pk, workflow_type, field_schema JSON per entity type) replaced hardcoded
    per-entity-type config in both `api/main.py` and both frontends. `GET
    /api/v1/governed-entities` serves the catalog. Adding a new reference entity or rule
    category is one registry row + creating its physical table, not edits across the API
    and both frontends.
  - *Done — Phase 2 (make a new rule type executable, not just storable).*
    `api/validation.py`'s `validate_record()` dispatches through `RULE_TYPE_HANDLERS`;
    `dbt_project/macros/rule_type_expr.sql` is the dbt-side handler registry (superseded
    `column_rule_flags.sql`, see above). `gold_survivorship_winners.sql` got a genuinely
    new rule_type, `shortest_value`, added as a single isolated `when` branch (plus the
    matching branch in `api/reprocessing.py`'s `SURVIVORSHIP_RANK_HANDLERS`). An
    unrecognized `rule_type` still fails loudly everywhere.
- **V4 domain-agnostic generalization** — see "V4: domain-agnostic generalization" above
  for the full scope. All six planned phases (metadata foundation, generic bronze/silver,
  generic gold, domain-scoped API, domain-agnostic frontend, full regression + docs) are
  complete as of this pass, including the maker-checker approval-flow end-to-end test and
  the Lineage tab fix. Product (or any second domain) getting a real pipeline was
  deliberately out of scope for that pass, kept metadata-only to prove the platform's
  shape without needing a second full dataset/rules configuration — see "Product domain
  onboarding" above for where that has since happened, and its own remaining gap
  (incremental loads, explicitly deferred).
- CCA-F (Claude Certified Architect – Foundations) certification is a separate, unrelated
  thread the user is pursuing — exam access is gated behind the Claude Partner Network with
  no individual registration path found so far; using Skilljar course-completion
  certificates as an interim credential.

## How to help in this project going forward

- Treat the working app as the source of truth; if asked to change behavior, check the
  actual current code/schema first rather than assuming from this summary alone (this
  document may lag recent changes).
- This is a demo built under real time constraints — prefer pragmatic, tested fixes over
  large rewrites unless explicitly asked to redesign something.
- Always test changes end-to-end (rebuild the pipeline, hit the actual API, click through
  the real UI with Playwright) before presenting a fix as done — this project's history
  includes more than one case where an untested "fix" shipped and had to be caught and
  corrected afterward.
- Windows portability matters for every file-path change — no hardcoded absolute paths.
- **No domain name may be hardcoded anywhere in code** — the whole point of the V4
  generalization. A new hardcoded "customer"/"Customer" string outside of comments,
  demo-data generators, or the `HEURISTIC_FALLBACK_DOMAINS` allowlist (which is itself
  explicit, documented, and about domain-specific AI-fallback heuristics, not a general
  exception) is a regression, not a shortcut.
- Do not assume anything about scope, which files to touch, or whether a change is safe —
  prompt the user if something is unclear, per this project's standing convention.
- Update documentation (`README.md`, this file, and the relevant `.docx` deliverables) as
  part of finishing a task once it's tested, not as a separate follow-up — but confirm
  with the user which docs to touch before editing any `.md`/`.docx` file, per this
  project's standing convention (list the files, then update once confirmed).
- **Git**: local checkpoint commits along the way are fine without asking. **Do not push
  to any remote** unless the user explicitly asks — for V4 specifically, the user has said
  they will create the GitHub repo and do the final push themselves once the project is
  ready; this is a change from V3's convention (documented in an earlier version of this
  file) of pushing automatically to `origin/main` on `MDM-medallion-demo-V3-With-MCP`. Do
  not revert to that behavior for V4.
