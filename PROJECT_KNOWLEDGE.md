# MDM Medallion Architecture Demo — Project Knowledge

Portfolio project for Balram's job search (targeting Senior MDM Architect / Director-level
data governance roles). Working demo, not production software — built to be a genuine
interview talking piece with runnable code behind it.

## Purpose & positioning

Demonstrates hands-on MDM architecture skills: metadata-driven data quality, match/merge
and survivorship, data stewardship workflows with AI assistance, lineage/impact analysis,
and access-controlled data products — the kinds of capabilities a Senior MDM Architect or
data governance Director is expected to have designed firsthand, not just managed.

## Architecture

Medallion architecture (bronze/silver/gold) with two synthetic heterogeneous source
systems (a CRM and an ERP), so schema standardization is a real problem, not a formality.

- **Bronze** — raw CRM + ERP extracts, tagged with source_system/source_record_id. Append-only
  (every load, full or incremental, inserts new rows rather than overwriting) — `bronze.load_control`
  (source_system, load_type, watermark_date, last_loaded_at, row_count) tracks each source's load
  history and drives auto-detection: no control row yet = full load, a control row exists =
  incremental (filtered to modified_date/last_updated ≥ the watermark). `scripts/load_bronze.py
  --force-full` overrides detection. A separate append-only log, `bronze.load_batches` (one row per
  `load_bronze.py` invocation, never updated — same convention as `audit.audit_trail`/
  `stewardship.match_review_log`), records the UUID shared by every row that run landed (all
  sources, full or incremental — `_load_batch_id`), the run's overall `load_type` ('FULL' if ANY
  source that run did a full load, else 'INCREMENTAL'), which sources it touched, and total row
  count. `stg_crm_customers.sql`/`stg_erp_customers.sql` are dbt `incremental` models
  (`unique_key='source_record_id'`, `incremental_strategy='delete+insert'`): on a full-refresh run
  they collapse bronze's entire history to one current row per key (latest `_load_ts`), exactly as
  before; on a plain incremental run they merge only the current batch (`macros/latest_load_batch.sql`
  reads `bronze.load_batches` at compile time to know which rows belong to it), instead of rescanning
  every row bronze has ever landed. `scripts/build_pipeline.py` reads `bronze.load_batches` itself to
  decide whether to pass `--full-refresh` to that dbt run. Logical deletes (`is_deleted`, sourced
  from `data/generate_source_data.py --incremental`) are carried through the staging layer
  unfiltered — an incoming `is_deleted=true` row simply upserts that key's existing staging row to
  reflect it — and filtered out downstream in `silver_all_staged.sql`, which (along with
  `silver_customers.sql`, `exceptions_queue.sql`, and every gold model) stays a full `table` rebuild
  on every run; that's what makes deletion-propagation and match-group recompute work correctly with
  zero special-case code — a deleted source record drops out of matching/survivorship on the next
  full recompute, letting a golden record re-survive from its remaining source (partial delete) or
  retire entirely once every contributing source is gone (full delete), verified end-to-end for both
  cases. This two-tier design (incremental staging, full-rebuild everything downstream) is
  deliberate: at this project's data volumes, only the bronze rescan was worth avoiding — see Known
  simplifications for why gold's own full-rebuild-every-run architecture, and the fuzzy tier's
  matching algorithm, are enterprise-scale limitations, not fixed by this change.
- **Silver** — canonical, validated records only. Cleansing/standardization/validation is
  **metadata-driven**: rules live in `bus_rules.column_rules` (rule_id, source column,
  rule_type, severity) — a DB-native table maintained via the Data Governance > Rules
  Configuration screen's maker-checker workflow, not a dbt seed. Records failing validation
  route to an exception queue instead of silver.
- **Gold (Data Hub)** — hybrid match/merge across sources, and **fully metadata-driven**:
  tier definitions/thresholds live in `bus_rules.matching_thresholds` and the
  fields/columns each tier operates on live in `bus_rules.matching_rules` (a
  child table keyed by tier_id) — nothing about *what* matches is hardcoded in pipeline
  code, mirroring the same pattern `bus_rules.column_rules` already established for silver
  validation. Today's configuration has two tiers: tier 1 is deterministic exact match on
  normalized email OR phone (confidence = that tier's `auto_merge_threshold`, 1.00); tier 2
  is an embedding-similarity fuzzy match (TF-IDF character n-gram cosine similarity over
  name/address text, blocked by state_code) that catches near-duplicates tier 1 misses.
  High-confidence fuzzy matches auto-merge with their similarity score as confidence;
  borderline ones surface in a **Match Review queue** for a data steward to confirm or
  reject rather than auto-merging. A third, non-tier row (`is_match_tier=false`)
  holds the 0.50 "provisional" baseline confidence assigned to a single-source golden
  record with no corroborating match at all — read by both the batch gold layer and
  real-time reprocessing, so they can't disagree on it. Computed in
  `scripts/generate_matches.py`, which runs as a Python step between silver and gold
  (dbt-duckdb can't do the embedding math in SQL) and writes to a `gold_prep` schema that
  the gold dbt models consume as a source — same "Python loads, dbt treats it as a source"
  pattern also used for bronze and for `ref`/`bus_rules` themselves. **Survivorship is
  attribute-level**, not record-level: each gold column is independently won by whichever
  contributing source best satisfies that column's own `bus_rules.survivorship_rules` rule
  (`most_common` / `most_complete` / `oldest` / `newest` / `pattern_match`), with
  `source_modified_date` desc (then CRM preferred) as the universal, non-configurable
  tie-break whenever a rule doesn't produce a clean winner — see
  `dbt_project/models/gold/gold_survivorship_winners.sql`. A **crosswalk table**
  (`gold_crosswalk`) preserves the relationship between every golden record and its
  contributing source records, with a graduated match confidence score (not hardcoded
  literals) plus a `winning_columns` list per source showing exactly which gold columns
  that source won (`is_survivor_record` is now simply "won at least one column").

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
- **Faker** generates the ~100-row-per-source synthetic population (see Key data model
  notes below for how single-source, exact-duplicate, and fuzzy-duplicate cases are seeded).
- **FastAPI** backend for all apps and the REST API, with bcrypt-hashed password auth
  (from-scratch bearer-token sessions, 8-hour expiry).
- Plain HTML/JS frontends (no build step, no Node.js needed) for both apps.
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

The batch pipeline (`scripts/build_pipeline.py`'s five steps) is also available as seven
stateless agents (`agents/`) coordinated by an MCP server acting as the central orchestrator
and tool registry (`mcp_server/`). Full design rationale, the data model, and end-to-end
verification results are in `MCP_Agent_Orchestration_Design.docx` — this section is a
summary, not the source of truth.

- **Ingestion / DQ / Matching / Survivorship agents** (`agents/ingestion_agent.py`,
  `dq_agent.py`, `matching_agent.py`, `survivorship_agent.py`) are thin, stateless wrappers
  around the exact same scripts/dbt commands `build_pipeline.py` already runs — no pipeline
  logic is reimplemented. `cancel_stale_match_reviews.py` runs inside Matching Agent (not a
  separate stage) and `audit_pipeline_diff.py` runs inside Survivorship Agent, both because
  `build_pipeline.py`'s own docstring requires them to run at those exact points for
  correctness — see the design doc's "Design Decisions" section for why.
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
- `mcp_server/tool_registry.py` is the canonical catalog (11 tools today); `mcp_server/server.py`
  registers them with the official `mcp` SDK's FastMCP server (stdio transport by default).
- **Verified end-to-end** (2026-08-07): `python -m agents.orchestration_agent --run-now` run
  in the real Windows venv — all 5 stages succeeded (real incremental bronze load, real dbt
  silver/gold runs, real `generate_matches.py` output, real audit diff), logged to
  `governance.orchestration_run_log` as run `712dee4d-980c-4f8d-959a-c97a44aa0235`.
- **Not yet done**: `api/main.py` wired through the MCP tool registry instead of importing
  `api/reprocessing.py` directly; `orchestration_schedule`/`monitoring_thresholds` exposed
  via the Rules Configuration screen; an actual OS-level Task Scheduler entry for the polling
  loop (today the pipeline has only been triggered manually via `--run-now`).

## Applications

1. **Data Stewardship console** (`/app/`) — four tabs.
   - **Dashboard** (landing tab on login) — one screen, two panes: Exceptions and
     Match Review. Each pane has KPI cards for its status breakdown (Exceptions:
     open/in_review/resolved/rejected; Match Review: pending/in_review/confirmed/
     rejected — Total is a sum, not a category, and isn't itself clickable), a
     bar chart (Exceptions: by rejection reason; Match Review: similarity-score
     buckets computed live off `bus_rules.matching_thresholds`, not hardcoded), a
     pie chart for Match Review's status breakdown, a "Your Activity" row
     (Submitted/Approved/Pending/Sent back, scoped to the logged-in user via
     `governance.workflow_instances.maker_user_id` — there's no per-user
     assignment on the queues themselves, so this is the only real per-user
     attribution available), and a trend chart (Exceptions: created vs. resolved
     vs. rejected per day, from `queued_ts` / `exception_status_overrides.updated_ts`).
     Every element except the trend chart is clickable and navigates to the
     Exception Queue or Match Review tab, pre-filtered to exactly that slice
     (`GET /api/v1/stewardship/queue` and `/stewardship/match-review` both accept
     `status` (incl. `all`), `reason`/`similarity_bucket`, or `activity` query
     params that compose against the same enriched view the dashboard summary
     itself queries, so a chart's count and its drill-through list can never
     disagree — verified end-to-end, not just by code review). Arriving via a
     drill-through shows a red "Filtered list → …" label plus a Clear button on
     the tab band (clicking Clear or a tab directly resets to the plain
     open/pending view); the label is currently shown for every drill-through,
     including Open/Pending — **whether to suppress it for Open/Pending is an
     explicit open decision the user deferred**, not yet resolved. Items in the
     resulting list that aren't `open` (Exceptions) or `pending` (Match Review)
     render read-only in the detail panel: no input fields, no action buttons,
     just a "Read-only · status: X" badge — the single-item GET endpoints
     (`/stewardship/queue/{id}`, `/stewardship/match-review/{pair_id}`) were
     extended to return `effective_status` for this, which they didn't before.
   - **Exception Queue** — reviews the exception queue, gets an
     AI-assisted remediation suggestion (real Claude API call via `ANTHROPIC_API_KEY`, with a
     transparent heuristic fallback when no key is set — every suggestion response includes
     `source: "ai"` or `"heuristic_fallback"`), and resolves/rejects corrections. **Resolving
     or rejecting now submits into the maker-checker workflow** (`stewardship_remediation`,
     1 Data Owner approver, different from the steward) instead of applying immediately —
     see the Maker-Checker Workflow Engine section below. Only once approved does
     resolving actually **trigger real-time reprocessing**: the corrected record is upserted
     into silver, matched against the current gold set using the same tier-1 exact_match_field
     rules from `matching_rules.csv` the batch pipeline reads (exact tier only, see known
     simplifications), and survivorship is re-run to either update the matched golden
     record or create a new one; a rejected approval returns the exception to the open queue.
     This mirrors the batch dbt logic so both paths stay consistent, though golden ID numbering
     can diverge between them (documented limitation).
   - **Match Review** — borderline fuzzy-match candidates from the gold layer's
     embedding-similarity tier (similarity between the review and auto-merge thresholds),
     shown side by side, for the steward to confirm or reject. Confirm/reject now also
     submits into a maker-checker workflow (`match_review_confirmation`, 2 sequential
     levels: Data Owner, then Admin) rather than recording immediately. Once a **confirm**
     decision is approved, it now **auto-processes in real time**
     (`api/reprocessing.py`'s `reprocess_confirmed_match_review`, called from
     `api/main.py`'s `_execute_match_review` on_approved callback) instead of only writing
     to `stewardship.match_review_overrides` and waiting for the next
     `python scripts/generate_matches.py && dbt run --select gold.*`: attribute-level
     survivorship (the same `bus_rules.survivorship_rules`-driven, per-column logic
     real-time exception reprocessing already uses) is re-run across the UNION of every
     source record already contributing to EITHER side's current golden record — not just
     the two paired records — producing the merged group's surviving value for every gold
     column except `golden_id` itself; the smaller (by numeric suffix) of the two sides'
     `golden_id`s survives; and the other is **retired**: its `main_gold.gold_customers`
     row is deleted and logged to the audit trail as `logically_deleted` (the same
     convention `scripts/audit_pipeline_diff.py` uses when a batch rebuild drops a
     `golden_id`), with every crosswalk row that pointed at either golden record rewritten
     to point at the surviving one. `stewardship.match_review_overrides` is still written
     to `'confirmed'` — that write happens BEFORE the merge is attempted, so the human
     decision stands (and stays queryable) even if the mechanical merge step itself fails;
     a failure surfaces in the workflow instance's `result.executor_error` rather than
     being silently lost, and the next full `generate_matches.py` run would still pick up
     the `'confirmed'` override as a fallback. A **reject** decision still has nothing to
     reprocess, in real time or in batch — rejecting never changes gold, it only
     permanently excludes the pair from future matching runs. If the two sides of a
     confirmed pair somehow already resolved to the same `golden_id` (e.g. an earlier
     reprocessing action already merged them via a different path), the merge step is a
     no-op (`action: "already_merged"`); verified end-to-end (real HTTP API, full 2-level
     approval chain, a synthetic cross-golden-record pair) both for the normal merge/retire
     path and this no-op path.
     **Match Review hold**: while a pair's review is open (`pending`/`in_review`, or
     the rare case of `confirmed` whose real-time merge itself failed and hasn't yet been
     realized by a later batch run), both of its two silver records
     sit out of matching entirely on every subsequent `generate_matches.py` run — they
     can't be auto-merged via a different path or drawn into an unrelated new suspect
     match while their fate is undecided. The pair's own queue row is carried forward
     frozen (not silently dropped) so it stays visible while held. If an updated version
     of either side's source record (same `source_system`+`source_record_id`) lands in
     silver while the review is still open, the review is auto-cancelled (status
     `cancelled`, a system-written note explaining why) rather than left to keep judging
     stale data — triggered in real time by steward reprocessing
     (`api/reprocessing.py`'s `_cancel_stale_match_reviews`) and in the batch pipeline by
     `scripts/cancel_stale_match_reviews.py` (diffs `main_silver.silver_customers`
     against a snapshot to catch a changed source row). A steward rejecting the pair, or
     the cancellation above, is what lifts the hold — a `confirmed` pair whose real-time
     merge already succeeded is recognized via `_cancel_stale_match_reviews`' own
     crosswalk-join check (both sides already share one `golden_id`) and treated as
     resolved, not held.
   - **Approvals** — a split-pane view (queue list left, detail panel right, same layout
     as Exception Queue/Match Review) listing workflow instances awaiting this user's
     decision ("Awaiting my decision") or previously submitted by them ("My
     submissions"), for any of the six workflow types whose current step matches their
     role. The list is grouped by category (workflow type label) then sorted by
     instance ID, and a category `<select>` above the list (rendered from
     `WORKFLOW_TYPE_LABELS`) narrows it to one workflow type at a time — both sort and
     filter are applied client-side over the *whole* result set (fetched with a high
     `limit`, not the pager's own page size) so grouping stays correct across pages,
     not just within whatever slice the server's own `limit`/`offset` happened to
     return. Selecting a row shows the submitter's name under "SUBMITTED BY", then an
     embedded, read-only view of the actual record the workflow concerns, framed in a
     bordered box with light field panes (a deliberate contrast against the console's
     dark theme) — for `stewardship_remediation`/`match_review_confirmation` this
     reuses the exact same rendering as the Exception Queue/Match Review tabs
     (`exceptionRecordFormHtml`/`matchRecordFormHtml`); the other four workflow types
     (`gold_record_edit`, `user_admin_change`, `reference_data_change`,
     `rules_config_change`) get an equivalent field-grid built from a per-type field
     list (`GOLD_FIELD_LABELS`/`USER_FIELD_LABELS`/`REFDATA_FIELD_LABELS`/
     `RULES_FIELD_LABELS`). Any field the submission actually changes is highlighted
     (darker green field background) with the prior value shown below in red,
     `Was: {old value}`; a create action (no prior record to diff against) shows a
     "New — nothing to compare against" badge instead. Backed by the same generic
     `GET /api/v1/workflows/pending` / `GET /api/v1/workflows/mine` /
     `GET /api/v1/workflows/{instance_id}` / `POST /api/v1/workflows/{instance_id}/decide`
     endpoints the Portal's Approvals view uses — plus, for the embedded record panel,
     a follow-up fetch of the affected entity itself (`/stewardship/queue/{id}`,
     `/stewardship/match-review/{pair_id}`, `/admin/users`, `/reference-data/{type}`,
     `/rules-config/{type}`, or the `gold_record_edit` payload's own embedded
     `old_snapshot` — no extra fetch needed there).

2. **MDM Data Hub Portal** (`/portal/`) — login-gated, SSO'd with the Stewardship
   app. Roles are `admin` / `dataSteward` / `dataOwner` / `businessUser`; gold
   access is `read_write` / `read` / `none`. `admin` is a distinct governance
   function (user administration only) and is deliberately **not** a superset of
   stewardship rights — only `dataSteward`/`dataOwner` can reach the Stewardship
   console or its API endpoints (enforced both server-side in `api/auth.py` and
   independently in the Stewardship frontend). Has:
   - **Browse Customers** — searchable/paginated gold-layer browser with live (debounced)
     search-as-you-type, click-through detail view with the source crosswalk, inline edit
     if the user's account has `read_write` gold access, and an **Audit Trail** button
     opening a read-only history panel for that golden record (see below). **An inline
     edit now submits into the `gold_record_edit` maker-checker workflow** (1 level, a
     quorum of 2 *different* Data Owners) instead of applying immediately — the response
     is `{status: "pending_approval", workflow_instance_id, steps}` rather than the
     updated record, and the audit-trail entry (once it does apply) still attributes the
     edit to the maker, not the approvers. **A golden record currently sitting in a
     pending Match Review pair is locked read-only here** — an amber "Under review"
     badge on the row, fields disabled and Save hidden in the detail modal regardless of
     `gold_access`, and `PUT /api/v1/customers/{golden_id}` itself returns 409 if called
     directly, so the lock holds even if the UI is bypassed. The flag
     (`pending_match_review`) is computed live in `_CUSTOMERS_ENRICHED_CTE` — join
     `gold_crosswalk` to `gold_match_review_queue` on either side of a pair, `COALESCE`
     against `stewardship.match_review_overrides` the same way the Match Review queue
     endpoints themselves resolve status — so it clears the instant a steward's
     confirm/reject clears the `match_review_confirmation` maker-checker chain, no extra
     code needed. Deliberately doesn't name the paired golden record or say "suspect" /
     "duplicate" in the UI copy itself, only on hover.
   - **Approvals** — a nav item visible to every signed-in role (not just
     dataSteward/dataOwner), since Admins — who approve Match Review's second level and
     User Administration changes — don't have access to the Stewardship Console.
     Rebuilt to match the console's Approvals tab exactly: the same split-pane
     list/detail layout (previously a table + popup modal), the same category
     filter/sort, and the same embedded, diff-highlighted record panel per workflow
     type — see the console's Approvals bullet above for the full behavior; the two
     copies share function names/logic verbatim (`sortApprovalItems`,
     `entityFieldsFormHtml`, `renderApprovalRecordSection`, `fetchApprovalRecordData`,
     etc.), duplicated per the no-shared-JS-file convention, not two designs that
     happen to look similar.
   - **Audit Trail** — append-only history of every golden record: creation, every edit
     (manual portal edit, steward real-time reprocessing, or batch pipeline recompute),
     and logical deletes (a golden_id no longer produced by a pipeline rebuild). Opens
     from a button on the customer detail modal as a separate panel showing the record's
     key identifiers up top and a newest-first timeline below, grouped into one entry per
     logical change with expandable old→new field diffs. Viewable by anyone with gold
     `read` or `read_write` access (`GET /api/v1/customers/{golden_id}/audit-trail`); there
     is no write/delete endpoint for it by design — no code path anywhere updates or
     deletes an audit row once written, which is the actual enforcement mechanism (DuckDB
     has no per-table grants to lean on here, same app-level-only security model as
     `gold_access` itself). See `api/audit.py` and Key data model notes below.
   - **Data Governance** (nav dropdown, four items; the first two are role-gated, the
     last two are visible to every signed-in user since they're read-only outside
     dataSteward/dataOwner — the dropdown itself is therefore always shown now):
     - **Data Stewardship** — visible only to `dataSteward`/`dataOwner` (same role gate
       as the console). Opens `/app/` via `window.open('/app/', 'mdmStewardshipTab')`;
       clicking again while that named tab is still open re-focuses it instead of
       spawning a duplicate — the "launch if not open, else shift control to it"
       behavior, implemented with the browser's native named-window targeting rather
       than any cross-tab messaging. Only recognizes tabs opened via this button.
     - **Lineage and Impact Analysis** — visible only with gold `read`/`read_write`
       access; this gates the nav item only — `/api/v1/lineage/*` stays open to any
       authenticated user regardless of `gold_access` (deliberate, pre-existing:
       pipeline metadata, not gold customer data). A full pannable/zoomable **network
       diagram** of the entire lineage metadata graph (bronze→silver→gold→stewardship).
       Click any node to highlight its upstream lineage (amber) and downstream impact
       (green) directly on the graph, with a side detail panel (description, direct
       in/out rules, upstream/downstream counts). Clicking a gold node adds a Golden ID
       lookup tool.
     - **Reference Data Maintenance** — Country Codes and State Codes (`ref.ref_country_codes`
       / `ref.ref_state_codes`, DB-native, each with a human-readable name/label column and
       an `is_active` flag; no hard delete, same "deactivate" convention as everywhere else
       in this app). Visible to everyone; create/edit/deactivate forms are only shown to
       `dataSteward`/`dataOwner` (everyone else gets a read-only table) and submit into the
       `reference_data_change` maker-checker workflow (1 level, 1 Data Owner). dbt's silver
       staging models (`stg_crm_customers.sql`, `stg_erp_customers.sql`) read these two
       tables as a `{{ source('ref', ...) }}` instead of a seed, so an approved edit here
       takes effect on the next pipeline rebuild.
     - **Rules Configuration** — Column Rules, Matching Rules (plus their tier definitions,
       "Matching Tiers"), and the new **Survivorship Rules** (`bus_rules.column_rules` /
       `matching_rules` / `matching_thresholds` / `survivorship_rules`, all DB-native).
       Same visibility/gating pattern as Reference Data Maintenance, but submissions go
       into the higher-risk `rules_config_change` workflow (1 level, quorum of 2 *different*
       Data Owners) since these rules govern what the batch pipeline rejects, matches, and
       survives. One workflow_type covers all four sub-tables; `entity_type` on the
       workflow instance distinguishes which one a given submission targets. dbt's
       `gold_crosswalk.sql` and `gold_survivorship_winners.sql` read `bus_rules.*` as
       sources; `scripts/generate_matches.py` and `api/reprocessing.py` query them directly.
       Both screens (tabs, columns, create/edit forms) and both apps' Approvals diff-views now
       render entirely from `governance.entity_type_registry` (`GET /api/v1/governed-entities`)
       rather than hardcoded per-entity-type config — see Outstanding / not yet built below.
   - **Administration → User Administration** — admin-only. Create/edit/deactivate users,
     assign role (`admin`/`dataSteward`/`dataOwner`/`businessUser`) and gold access
     (`read_write`/`read`/`none`), reset passwords. The seed admin account is `mdm_admin`, created via
     `scripts/create_admin_user.py`, which prints a one-time random password to the
     terminal — never stored in plaintext, never baked into the shipped zip. **Creating a
     user, or changing `role`/`gold_access`/`is_active` on an existing one, now submits
     into the `user_admin_change` maker-checker workflow** (1 level, 1 Admin approver who
     must be a different admin than the requester) — the new-user's one-time password is
     generated only at approval time and shown only to the approver, not the requester.
     `full_name`-only edits and password resets are not gated (they don't change what a
     user is authorized to do) and still apply immediately.

3. **Maker-Checker Approval Workflow Engine** (`api/workflow_engine.py`, `governance`
   schema) — generic across all three apps, added so a maker can never be the sole
   authority on a state-changing action. A `workflow_type` is an ordered list of steps
   (`governance.workflow_definitions`, seeded once on first boot and left alone
   thereafter so a future Rules Configuration screen can edit it at runtime); each step
   names a `required_role` and an `approvals_required` count (1 for a simple single
   approver, >1 for a same-level quorum of *different* people). The engine itself
   enforces, independent of any caller: a maker can never decide on their own
   submission, the same approver can never cast two decisions on one instance, and
   rejection at any step is terminal. Six workflows are configured today:
   `stewardship_remediation` (1 level, 1 Data Owner), `gold_record_edit` (1 level,
   quorum of 2 Data Owners), `match_review_confirmation` (2 sequential levels: Data
   Owner, then Admin), `user_admin_change` (1 level, 1 Admin), `reference_data_change`
   (1 level, 1 Data Owner — Reference Data Maintenance), and `rules_config_change`
   (1 level, quorum of 2 Data Owners — Rules Configuration). Generic endpoints —
   `GET /api/v1/workflows/pending`, `GET /api/v1/workflows/mine`,
   `GET /api/v1/workflows/{instance_id}`, `POST /api/v1/workflows/{instance_id}/decide`
   — serve an **Approvals** view in both the Stewardship Console and the Portal (the
   Portal's copy is visible to every role, since Admins can't reach the console).
   `scripts/create_demo_governance_users.py` seeds the extra `dataOwner`/`admin`
   accounts (`mdm_dataowner2`, `mdm_dataowner3`, `mdm_admin2`) needed to actually clear
   a quorum or a "different approver" requirement in a fresh install, which otherwise
   ships with only one account per role.

4. **Pagination** — every list-backed `GET` endpoint (`/customers`, `/admin/users`,
   `/reference-data/{entity_type}`, `/rules-config/{entity_type}`, `/workflows/pending`,
   `/workflows/mine`, `/stewardship/queue`, `/stewardship/match-review`,
   `/customers/{golden_id}/audit-trail`) takes `limit`/`offset` (default `limit=10`) and
   echoes `total`/`limit`/`offset` back alongside the page (field name varies:
   `customers` on `/customers`, `users` on `/admin/users`, `items` everywhere else;
   `/stewardship/queue` and `/stewardship/match-review` also keep `count` as an alias
   for `total`, predating this change). Endpoints that build their result set as a
   DataFrame first (the queue/match-review status/reason/activity/bucket branches)
   slice with `_paginate_df`; endpoints whose data comes back as a plain Python list
   (`/workflows/pending`, `/workflows/mine` — quorum-aware filtering happens in
   `workflow_engine.py`, not SQL) slice with `_paginate_list`. The audit-trail endpoint
   is the one exception to "paginate the raw rows": it groups by `change_batch_id`
   first and paginates *that* (one page item = one logical edit, however many fields
   it touched), so a page can never split one edit's field-diff rows across two pages.
   On the frontend, every list view in both apps shares one small pagination module
   (`pagerState`/`pagedFetch`/`pagerHtml`/`pagerGo`/`pagerSetSize`, duplicated verbatim
   in both `index.html`s per the no-shared-JS-file convention): Prev/Next, a
   10/25/50/100 page-size `<select>`, and a `total` count, with the chosen page size
   remembered in `localStorage` per view key (e.g. `mdm_pagesize_portal_browse`). If a
   filter, tab switch, or search shrinks the result set out from under the current
   page, `pagedFetch` re-fetches page 1 of the corrected range automatically rather
   than rendering a blank list.

5. **REST API** — exposes the gold layer, crosswalk, lineage graph, and stewardship queue
   for downstream consumption. Auto-docs at `/docs`.

## Key data model notes

- `bus_rules.column_rules` — the single source of truth for cleansing/standardization/
  validation rules, referenced by rule ID (R001, R002...) throughout the pipeline, the
  exception queue's reject_reasons, and the stewardship UI. DB-native (schema `bus_rules`),
  maintained via the Rules Configuration screen's maker-checker workflow — migrated off
  `dbt_project/seeds/column_rules.csv` when that screen was built.
- `dbt_project/seeds/lineage_edges.csv` — column-level lineage metadata (from_layer/table/
  column → to_layer/table/column, transform_rule_id, description). Powers both the
  Data Governance network diagram and the `/api/v1/lineage/*` endpoints. Still a dbt seed
  (out of scope for the Rules Configuration migration). Note: wildcard nodes (e.g.
  `gold.gold_match_candidates.*`) must be explicitly linked to specific columns or
  impact-analysis chains silently break — this bit us once already, and again when the
  match/merge step moved into `gold_prep` (matching layer is now `gold_prep`, edges
  E013/E014/E018-E026 cover it; re-verify with `/api/v1/lineage/impact` and `/trace` after
  touching this file, don't just eyeball it).
- `bus_rules.matching_thresholds` — one row per matching tier (tier_id, tier_order,
  tier_name, match_method, is_match_tier, auto_merge_threshold, review_lower_threshold,
  active, description). DB-native, maintained via Rules Configuration's "Matching Tiers"
  tab (migrated off `dbt_project/seeds/matching_thresholds.csv`). `match_method` is
  `'exact'` or `'fuzzy_tfidf_cosine'` today; a non-tier row (`is_match_tier=false`,
  `match_method='no_match_baseline'`) holds the 0.50 provisional-confidence value.
  Adding a tier means adding a row here (plus its rules below) — nothing in
  `scripts/generate_matches.py` or `api/reprocessing.py` needs to change to add another
  exact or fuzzy tier, only to add a genuinely new match_method.
- `bus_rules.matching_rules` — child table keyed by `tier_id`, one row per field/column a
  tier operates on (rule_role is `exact_match_field`, `similarity_text_field`, or
  `blocking_key`; `transform_function` is `none`/`normalize_email`/`normalize_phone`,
  applied identically in both Python (`scripts/generate_matches.py`, `api/reprocessing.py`)
  and SQL (`gold_crosswalk.sql`'s tier lookups) via matching name-keyed registries). DB-native,
  maintained via Rules Configuration's "Matching Rules" tab (migrated off
  `dbt_project/seeds/matching_rules.csv`). Multiple `blocking_key` rows for one tier compose
  into a multi-column blocking key; `rule_order` controls concatenation order for
  `similarity_text_field` rows.
- `bus_rules.survivorship_rules` — new: one active rule per gold column (`target_column`),
  driving attribute-level survivorship. `rule_type` is `most_common` (the non-blank value
  appearing in the most contributing records), `most_complete` (prefers non-blank over
  blank), `oldest`/`newest` (by `source_modified_date`), or `pattern_match` (prefers a value
  matching `rule_param`, a regex). Exactly one primary rule per column; whenever it doesn't
  produce a clean winner, `source_modified_date` desc (then CRM preferred) is the universal,
  non-configurable tie-break — not itself a further stack of configurable rules. Defaults
  (seeded by `api/db.py`/`scripts/ensure_governed_schemas.py`) are all `newest`, which
  reproduces the previous record-level behavior exactly until a governance user tunes a
  specific column. Evaluated by `dbt_project/models/gold/gold_survivorship_winners.sql`
  (batch) and `api/reprocessing.py`'s `_pick_column_winners()` (real-time) — the two must
  stay in lockstep.
- `ref.ref_country_codes` / `ref.ref_state_codes` — DB-native reference data (schema `ref`),
  maintained via the Reference Data Maintenance screen's maker-checker workflow. Each has a
  human-readable `country_name`/`state_name` label alongside the code, plus `is_active`
  (migrated off `dbt_project/seeds/ref_country_codes.csv` / `ref_state_codes.csv`, which
  only had the bare code). `api/validation.py` queries these live (no caching) since, unlike
  the old static seed, they can change at runtime.
- `gold_prep.match_groups` / `match_edges` / `match_review_candidates` — written by
  `scripts/generate_matches.py` (not dbt-built), same "Python loads, dbt treats it as a
  source" pattern as bronze. `match_edges` is the audit trail dbt joins to compute
  `gold_crosswalk.match_confidence_score`.
- `auth.users` / `auth.sessions` — bcrypt password hashes, bearer tokens, 8hr TTL.
- `stewardship.remediation_log` / `remediated_records` / `exception_status_overrides` —
  audit trail and status tracking layered on top of the dbt-built `exceptions_queue` table
  (which itself is a static point-in-time snapshot; live status lives in the overrides
  table, not the snapshot).
- `stewardship.match_review_overrides` / `match_review_log` — same live-status-overlay
  pattern, for `gold_match_review_queue` instead of `exceptions_queue`. A 'confirmed'
  override is realized in real time by `api/reprocessing.py`'s
  `reprocess_confirmed_match_review` the moment the maker-checker chain approves it
  (see the Match Review bullet above); `generate_matches.py`'s forced union-find merge on
  the next batch run is now just a fallback for the rare case that real-time step itself
  failed (the merge is idempotent either way — a pair whose sides already share a
  `golden_id` is a no-op). A 'rejected' override permanently excludes that pair from ever
  resurfacing. 'cancelled' (new) is a system-written terminal status distinct from
  'rejected' — see "Match Review hold" above.
- `stewardship.silver_customers_snapshot` — new: point-in-time snapshot of
  `main_silver.silver_customers`' identity + `source_modified_date`, refreshed at the end
  of every `scripts/cancel_stale_match_reviews.py` run. Silver has no other diff
  mechanism (dbt `CREATE OR REPLACE`s the whole table every run) — this is how that
  script detects "an updated version of this source+SourceID landed in silver" across a
  batch rebuild, same reason `audit.gold_customers_snapshot` exists for the gold layer.
- `audit.audit_trail` — append-only gold-layer audit trail (`api/audit.py`). One row per
  changed field per logical operation, grouped by `change_batch_id` (all field rows from
  one edit share a batch id so the UI renders one timeline entry per edit). Three writers:
  `api/main.py`'s `update_customer()` (`event_source='portal_manual_edit'`),
  `api/reprocessing.py` (`'steward_reprocessing'`), and `scripts/audit_pipeline_diff.py`
  (`'pipeline_batch'`, run as the final step of `scripts/build_pipeline.py`). No code
  anywhere issues an UPDATE/DELETE against this table.
- `governance.workflow_definitions` / `workflow_instances` / `workflow_decisions` — the
  maker-checker engine's own tables (`api/workflow_engine.py`). Definitions are seeded
  per `workflow_type` (an existing type is never re-seeded, but a newly-added one — like
  `reference_data_change`/`rules_config_change` when Rules Configuration shipped — is
  backfilled into an already-running DB on next app start) so runtime edits persist across
  restarts; instances carry a JSON `payload`
  (whatever the completion callback needs to apply the change) and, once
  approved/rejected, a JSON `result`; decisions are one row per approver per step, which
  is how the engine enforces "no double-deciding" and quorum counts.
- `audit.gold_customers_snapshot` — a copy of `main_gold.gold_customers`'s tracked columns
  as of the last batch pipeline run, kept in the `audit` schema specifically because `dbt
  run` never touches that schema (it CREATE OR REPLACEs `main_gold.gold_customers` every
  run). `scripts/audit_pipeline_diff.py` diffs the fresh gold table against this snapshot
  to detect batch-driven creates/updates/logical-deletes, then refreshes it.

## Known simplifications (documented on purpose, not oversights)

- Fuzzy matching (the TF-IDF similarity scoring itself, i.e. finding NEW borderline
  candidates) only runs in the batch pipeline (`scripts/generate_matches.py`).
  Real-time reprocessing (steward resolves an exception → immediate re-match) still only
  evaluates the `match_method='exact'` tier -- fitting a TF-IDF vectorizer per API request
  was judged not worth the latency for a demo-scoped feature. Both paths read the same
  `exact_match_field` rules from `bus_rules.matching_rules`, so they can't drift on *which*
  fields count as an exact match, even though only the batch step can also run a fuzzy
  tier. This is unrelated to *realizing* an already-decided Match Review confirmation,
  which is a merge/survivorship operation on a known pair, not a search for new candidates
  -- that now happens in real time (`api/reprocessing.py`'s
  `reprocess_confirmed_match_review`, see the Match Review bullet above), not on the next
  batch rebuild. A rejected decision has nothing to reprocess either way.
- The fuzzy tier is TF-IDF character n-gram cosine similarity, not a neural sentence
  embedding -- a deliberate lightweight choice (see Tech stack); the vectorizer's shape
  (char_wb analyzer, 2-4 char n-grams) is a fixed code constant in `generate_matches.py`,
  not rule metadata. Thresholds (0.80 auto-merge, 0.35 review floor today) live in
  `bus_rules.matching_thresholds`'s `auto_merge_threshold`/`review_lower_threshold` columns
  for the fuzzy tier row, not hardcoded constants -- edit the row (via Rules Configuration,
  maker-checker approved) and rerun to recalibrate, no code edit needed. Original
  calibration was empirical, against this project's synthetic data generator's 12 seeded
  fuzzy-duplicate pairs vs. every same-state non-duplicate pair; re-calibrate if the
  generator's population changes materially.
- Attribute-level survivorship's `most_common`/`most_complete`/`pattern_match` rule types
  are evaluated independently per column across whatever sources contributed to that match
  group -- there's no cross-column consistency check (e.g. a golden record could in theory
  draw `city` from one source and `state_code` from another even if that pairing wouldn't
  occur together in any single real source record). Accepted for a demo-scoped feature;
  a production system would likely want a "prefer values from the same winning source when
  reasonably possible" refinement.
- Steward corrections ARE re-validated against the reject-severity metadata rules
  (`api/validation.py`, mirroring `column_rules.csv`) before reprocessing runs — a
  correction that still fails validation is rejected with an alert and the record
  stays in the queue (status remains `open`). Only a genuinely valid correction is
  treated as authoritative and flows into match/merge; correct-severity
  standardization rules (proper-casing, phone formatting) are not re-checked, since
  they don't gate silver eligibility in the batch pipeline either.
- Real-time reprocessing's match step compares against each golden record's *current*
  survivor values, not every historical non-survivor source in that group.
- New golden IDs from real-time reprocessing increment the current max, which can diverge
  from what a full `dbt run` would assign from scratch. The audit trail inherits this: if
  a reprocessing-created golden_id gets renumbered by the next full rebuild,
  `audit_pipeline_diff.py` logs the old number as `logically_deleted` and the new number
  as `created`, even though it's the same underlying customer — not a bug in the diff
  logic, an accepted consequence of the numbering scheme itself. The same
  renumbering-by-position can happen purely within batch-to-batch dbt rebuilds too, with no
  real-time reprocessing involved at all: `match_group_id` is a dense enumeration over
  clusters sorted by each cluster's lowest (source_system, source_record_id) member, so
  removing or adding a match group shifts every group sorting after it by one position.
  Since `audit_pipeline_diff.py` diffs by golden_id string, a shifted group's OLD golden_id
  string gets reassigned to a DIFFERENT underlying customer, which the diff logs as
  "updated" (every tracked field differing) rather than a delete+create pair — confirmed
  empirically when testing the incremental staging models below (a single logical delete
  produced dozens of "updated" rows purely from this renumbering cascade, not from those
  customers' actual data changing).
- Incremental silver staging (`stg_crm_customers.sql`/`stg_erp_customers.sql`, dbt
  `incremental` materialization) only avoids rescanning bronze's full history — it
  deliberately does NOT extend to `silver_all_staged.sql`/`silver_customers.sql`/
  `exceptions_queue.sql` or any gold model, which all stay full-`table` rebuilds every run.
  That's what makes deletion-propagation and match-group recompute work with zero
  special-case code (see the Bronze bullet under Architecture), but it also means gold's
  own rebuild cost, and `scripts/generate_matches.py`'s fuzzy-tier matching cost, are both
  still proportional to total data size, not to what changed. Fine at this project's
  volumes; not a solution to the enterprise-scale gap noted below.
- **Enterprise-scale limitation, not addressed by the incremental staging work above**: at
  real enterprise volumes (tens to hundreds of millions of records), two separate things
  break, neither fixed by making staging incremental. (1) The fuzzy-matching tier's
  blocking step does an exhaustive pairwise comparison within each block
  (`scripts/generate_matches.py`) — a block of even a few million records makes that
  comparison count infeasible in a single-node Python process, regardless of full vs.
  incremental; a production system would need approximate similarity search (LSH, a vector
  index) or a dedicated entity-resolution engine (Zingg, Senzing, AWS Entity Resolution, a
  commercial MDM platform's matching layer), not exhaustive blocking-and-compare. (2) Gold
  (and everything downstream of staging) is deliberately rebuilt from scratch every run —
  correct and cheap at ~200 rows, but proportional to total data size at scale. A
  production system would need genuinely incremental, affected-group-only recompute in gold
  (using blocking keys and existing crosswalk membership to identify which specific golden
  records a change could touch, not reclustering the whole population), on top of replacing
  the single-file DuckDB store with a real warehouse/lakehouse. Documented explicitly as a
  gap in `Enterprise_Readiness_Assessment.docx`.
- The audit trail only captures a manual `dbt run --select gold.*` if
  `scripts/audit_pipeline_diff.py` is run immediately afterward (this happens
  automatically inside `build_pipeline.py`, but not if you run the gold rebuild command
  by itself, e.g. after a Match Review confirm/reject).
- Auth is sized for a demo: no MFA, no refresh tokens, single local DuckDB file (not a
  concurrent multi-user warehouse).
- If the domain action itself fails after every required
  approval is already recorded (e.g. a downstream write error), the instance is still
  marked `approved` — the human decision stands — with the failure captured in its
  `result` field rather than silently rolled back; there is no automatic retry. A
  quorum or multi-level chain also needs enough distinct people in the required role to
  ever clear, since a maker can't approve their own submission and the same approver
  can't decide twice on one instance — hence `scripts/create_demo_governance_users.py`.

## Repo structure

```
data/                  synthetic CRM+ERP source data generator (Faker-based, ~100 rows/source)
scripts/                load_bronze.py (also writes bronze.load_batches, an append-only
                        log of every ingest run, read by the silver incremental models and
                        by build_pipeline.py), generate_matches.py (fuzzy match/merge,
                        Python step between silver and gold; also enforces the Match Review
                        hold), cancel_stale_match_reviews.py (batch-path trigger for
                        cancelling a stale Match Review pair, runs between the silver dbt
                        run and generate_matches.py), build_pipeline.py (runs the 7-stage
                        build, step 0 = ensure_governed_schemas.py; reads
                        bronze.load_batches before the silver dbt run to decide whether to
                        pass --full-refresh), ensure_governed_schemas.py
                        (provisions ref.*/bus_rules.*/governance.entity_type_registry before
                        dbt runs -- see its docstring),
                        audit_pipeline_diff.py (gold-layer audit trail diff, final build
                        step), create_admin_user.py, reset_admin_password.py,
                        create_demo_governance_users.py (seeds extra dataOwner/admin
                        accounts needed to test maker-checker quorums/multi-level chains)
dbt_project/            seeds (only lineage_edges.csv now -- column_rules.csv,
                        matching_thresholds.csv, matching_rules.csv, ref_state_codes.csv,
                        and ref_country_codes.csv migrated to DB-native bus_rules/ref
                        schemas when Rules Configuration/Reference Data Maintenance
                        shipped), models (silver, gold); gold reads
                        scripts/generate_matches.py's output via the gold_prep source and
                        bus_rules.*/ref.* via their own sources. stg_crm_customers.sql /
                        stg_erp_customers.sql are dbt incremental models (unique_key=
                        source_record_id, delete+insert) -- everything else in silver and
                        all of gold stays full-table-rebuild every run. macros/:
                        proper_case.sql (standardization), rule_type_expr.sql +
                        column_rule_flags.sql (Phase 2 -- bus_rules.column_rules.rule_type
                        handler registry, used by the silver staging models),
                        latest_load_batch.sql (compile-time lookup of the current bronze
                        ingest batch from bronze.load_batches, used by the two incremental
                        staging models' is_incremental() branch)
api/                    FastAPI app: main.py, db.py, auth.py, reprocessing.py,
                        lineage.py, ai_remediation.py, audit.py (gold-layer audit trail),
                        workflow_engine.py (generic maker-checker workflow engine)
stewardship_app/frontend/   exception queue + match review console (plain HTML/JS), plus
                        an Approvals tab over the maker-checker workflow engine
portal_app/frontend/        login-gated portal: browse, governance graph, admin, and an
                        Approvals view (plain HTML/JS)
requirements.txt        pinned deps (duckdb==1.5.4 -- NOT the same version number as the
                        dbt-duckdb adapter, a mistake made once already, worth double-checking)
README.md               Windows-first setup instructions (PowerShell), plus macOS/Linux
```

## Outstanding / not yet built

- Attribute-level survivorship's cross-column consistency (see Known simplifications) is
  an accepted gap, not a planned build item.
- **Reference Data Maintenance / Rules Configuration extensibility** — Phases 0, 1,
  and 2 of the plan discussed in chat are now all built (see below for what each
  covers and what's deliberately still out of scope).
  - *Done — Phase 0 (fail loudly).* `gold_survivorship_winners.sql`'s CASE statement now
    calls DuckDB's `error(...)` on an unrecognized `survivorship_rules.rule_type` instead
    of silently scoring like `most_common`; `scripts/generate_matches.py` now raises
    `RuntimeError` on an active `matching_thresholds` tier with an unrecognized
    `match_method`, or two active tiers sharing one method (previously silently excluded
    from matching, `next(...)` only ever finding the first). `api/main.py`'s
    `submit_rules_config_change` rejects (400) any `rule_type`/`match_method`/
    `transform_function` value with no real handler at submission time, before a Data
    Owner quorum can approve something that would do nothing.
  - *Done — Phase 1 (metadata-driven admin/CRUD layer).* `REFERENCE_ENTITY_CONFIG` /
    `RULES_ENTITY_CONFIG` (`api/main.py`) and the portal's `REF_DATA_TABS` / `RULES_TABS`
    / `REFDATA_FIELD_LABELS` / `RULES_FIELD_LABELS` JS (duplicated in both frontends) are
    gone, replaced by one table, `governance.entity_type_registry` (seeded in
    `scripts/ensure_governed_schemas.py` and mirrored in `api/db.py`, same duplication
    convention as `ref`/`bus_rules`) — one row per entity type: table name, pk, `workflow_
    type`, and a `field_schema` JSON (label/input-type/select-options per field, plus
    `role: pk`/`role: name` for reference-data code/label fields). `GET /api/v1/governed-
    entities` serves the catalog; both apps' Reference Data Maintenance, Rules
    Configuration, and Approvals diff-views render tabs/columns/forms from it generically.
    Adding a new reference entity or rule category is now one registry row + creating its
    physical table (same as any typed schema change), not edits across `api/main.py` and
    both frontends. Verified end-to-end by adding a throwaway `ref_currency_code` entity
    type and taking it through catalog listing, tab rendering, and full maker-checker
    approval with zero further code changes.
  - *Done — Phase 2 (make a new rule type executable, not just storable).*
    `api/validation.py`'s `validate_record()` now reads active, reject-severity
    `bus_rules.column_rules` rows and dispatches each through `RULE_TYPE_HANDLERS`
    (`not_null`/`regex`/`reference_state`/`reference_country`), the same
    handler-registry shape as `scripts/generate_matches.py`'s `TRANSFORM_FUNCTIONS` --
    editing a rule's `rule_param` (e.g. loosening the email regex) via Rules
    Configuration now takes effect immediately, no code change. The batch/dbt side got
    the equivalent treatment: two new macros, `dbt_project/macros/rule_type_expr.sql`
    (the handler registry -- one `{% elif %}` branch per `rule_type`) and
    `column_rule_flags.sql` (queries `bus_rules.column_rules` via `run_query` at
    *compile time* and generates the `err_*` boolean columns), wired into
    `stg_crm_customers.sql` and `stg_erp_customers.sql`. `gold_survivorship_winners.sql`
    got a genuinely new rule_type, `shortest_value`, added as a single isolated `when`
    branch (plus the matching branch in `api/reprocessing.py`'s new
    `SURVIVORSHIP_RANK_HANDLERS` registry for the real-time path, which had the exact
    same silent-fallthrough gap Phase 0 fixed on the dbt side -- `rank_key()` used to
    `return 0` for an unrecognized `rule_type` instead of raising).
    An unrecognized `rule_type` still fails loudly everywhere: `ValueError` in
    `validate_record`/`reprocessing.py`, `exceptions.raise_compiler_error` (a hard `dbt
    compile` failure) in the macros.
    Scope, deliberately not generalized: CRM's `err_missing_phone` stays hand-written
    (there's no `bus_rules.column_rules` row for CRM+phone at all, so there's nothing
    for the macro to read); ERP's `err_missing_first_name`/`err_missing_last_name`
    (R009, full_name) stay hand-written (full_name is split into first/last during
    standardization, so "is it missing" is a property of that split, not a plain
    not-null check on one staged column). `column_rule_flags()` raises a compiler
    error if a requested field has zero matching active rules, rather than silently
    defaulting to `false`, specifically to keep this boundary from being crossed by
    accident later. Standardization/correct-severity transforms (proper-casing, phone
    formatting, `lowercase`) were left out of this phase entirely -- still hardcoded,
    still cosmetic, still not gating silver eligibility either way. Verified
    end-to-end: a full `dbt run` against the real dataset produces byte-identical
    `err_*` flags to the pre-Phase-2 hand-written SQL across all 200 staged rows;
    `shortest_value` activated on `address_line1` correctly picked the shorter address
    in every one of 110 real match groups with more than one candidate; the real
    FastAPI server (logged in via a real token) shows `shortest_value` as a selectable
    `rule_type` option through `GET /api/v1/governed-entities` and reflects a live
    DB edit through `GET /api/v1/rules-config/survivorship_rule`.
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
- Always test changes end-to-end (rebuild the pipeline, hit the actual API, don't just
  review code) before presenting a fix as done — this project's history includes more than
  one case where an untested "fix" (a bad dependency pin, a missing `pandas` requirement,
  a broken lineage chain) shipped and had to be caught and corrected afterward.
- Windows portability matters for every file-path change — no hardcoded absolute paths.
- **Every time a fix or enhancement is successfully tested, update documentation and push
  to GitHub as part of finishing the task, not as a separate follow-up:** update this file
  (`PROJECT_KNOWLEDGE.md`), `README.md`, and the relevant `.docx` deliverables (at minimum
  `Design_Document.docx`; also `Enterprise_Readiness_Assessment.docx` and the
  `Installation_Guide_*.docx` files if the change affects them) so they describe the
  change, then `git add` / `git commit` / `git push` to `origin/main`
  (`https://github.com/balramvv-gh/MDM-medallion-demo-V3-With-MCP.git`). Check `git status`
  and `git log main..origin/main` first — this repo has previously accumulated uncommitted
  local changes and un-pulled remote commits from other sessions/machines; reconcile
  (`git fetch`, `git merge --ff-only origin/main` or resolve conflicts) before adding new
  work on top, rather than committing blind. Network access to github.com from a sandboxed
  session has been intermittent (occasional `502`/`403` from the proxy) but has succeeded
  on retry every time so far — retry a few times before concluding the push isn't possible.
