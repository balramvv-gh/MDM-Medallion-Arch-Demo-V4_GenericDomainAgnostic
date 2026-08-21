# MDM Medallion Architecture Demo

A working demonstration of a metadata-driven Master Data Management pipeline built on
a bronze/silver/gold (medallion) architecture, with an AI-assisted data stewardship
application, a login-gated data hub portal, and a REST API over the gold layer.

## What this demonstrates

- **Bronze layer** — raw customer extracts from two heterogeneous source systems (a
  CRM and an ERP), landed untouched.
- **Silver layer** — cleansing, standardization, and validation driven entirely by
  **metadata** (`bus_rules.column_rules`, a DB-native table maintained via the Data
  Governance > Rules Configuration screen), not hardcoded logic. Records
  that fail validation are routed to an exception queue instead of the pipeline.
- **Gold layer (Data Hub)** — hybrid match/merge across source systems, **entirely
  metadata-driven**: tiers and thresholds live in `bus_rules.matching_thresholds`,
  and the columns each tier compares live in `bus_rules.matching_rules` — the
  same "rules live as governed metadata, not in code" pattern as `bus_rules.column_rules`.
  Today's configuration has deterministic exact matching on normalized email/phone, plus an
  **embedding-similarity (fuzzy) tier** that catches near-duplicates exact
  matching misses (typos, nicknames, reformatted addresses) via TF-IDF
  character n-gram cosine similarity, computed in `scripts/generate_matches.py`
  and clustered with union-find. High-confidence fuzzy matches auto-merge;
  borderline ones go to a **Match Review queue** for a data steward to confirm
  or reject. **Survivorship is attribute-level**: each gold column is independently won
  by whichever contributing source best satisfies that column's own rule in
  `bus_rules.survivorship_rules` (most common / most complete / oldest / newest / pattern
  match), tie-broken by most-recently-modified (then CRM preferred). A **crosswalk table**
  preserves the link between every golden record and its contributing source records —
  including, per source, exactly which gold columns it won — with
  a graduated match confidence score (that tier's `auto_merge_threshold` for exact,
  the actual similarity score for fuzzy, and a 0.50 "provisional" baseline
  for uncorroborated single-source records) — all sourced from governed metadata, not
  hardcoded literals.
- **Data Stewardship app** (`/app/`) — restricted to `dataSteward` / `dataOwner`
  accounts, four tabs. A **Dashboard** (the landing tab on login) gives an
  at-a-glance view of both queues: status/reason/trend charts for exceptions and
  status/similarity-bucket charts for Match Review, plus a "your activity" count
  of what you've personally submitted/had approved/sent back. Every chart element
  and count is clickable — it jumps to the corresponding Exception Queue or Match
  Review tab pre-filtered to just that slice (status, reason, similarity bucket,
  or your activity), with a red "Filtered list → …" label and a Clear button on
  the tab band; items outside the actionable status (`open`/`pending`) render
  read-only, with no edit fields or action buttons. Then an **Exception Queue**
  console for reviewing rejected records,
  getting an AI-assisted remediation suggestion (Claude, with a heuristic fallback
  if no API key is configured), and approving/rejecting corrections. Approving a
  correction triggers real-time **reprocessing**: the corrected record is upserted
  into the silver layer, matched against the current set of golden records, and
  survivorship is re-run to either update the matched golden record (recomputing
  the survivor across the whole contributing group) or create a brand-new golden
  record if no match is found. The console shows you which happened and which
  `golden_id` was affected. And a **Match Review** tab: borderline fuzzy-match
  candidates from the gold layer's embedding-similarity tier, shown side by
  side, for the steward to confirm ("same customer, merge them") or reject
  ("coincidence, keep separate"). A confirm decision, once it clears its
  maker-checker approval chain, now **reprocesses in real time**, same as
  exception resolution: attribute-level survivorship is re-run across every
  source record contributing to either side's golden record, the smaller of
  the two `golden_id`s survives, and the other is retired (see
  `api/reprocessing.py`'s `reprocess_confirmed_match_review`) rather than
  waiting for the next `generate_matches.py && dbt run --select gold.*`.
- **MDM Data Hub Portal** (`/portal/`) — a **login-gated** application whose landing
  screen is now a **Dashboard**: one pane of gold-layer analytics (source count —
  single- vs. multi-source, match confidence tier, count by survivor source system,
  state/country geography, pending-approval status, and a created/updated/logically-
  deleted trend from the audit trail). Every element is clickable and jumps to
  **Browse Customers** — the gold layer (search, paginate, view a record's full
  source crosswalk, and edit it if your account has write access) — pre-filtered to
  that slice, with the same red "Filtered list → …" label + Clear button pattern as
  the Stewardship Dashboard. A golden record awaiting a data steward's decision on a
  likely duplicate match is shown as "Under review" and locked read-only until that's
  resolved, even for accounts with write access. The top nav (including the Data
  Governance and Administration dropdown triggers) now uses the same underline-tab style as the
  Stewardship console. Plus an **Administration → User
  Administration** screen for managing portal accounts: creating users, assigning
  roles (`admin` / `dataSteward` / `dataOwner` / `businessUser`) and gold-layer access
  (`read_write` / `read` / `none`), deactivating accounts, and resetting passwords.
  Passwords are stored as bcrypt hashes — never in plaintext. Note: `admin` is a
  distinct governance function (user administration) and is deliberately **not** a
  superset of stewardship rights — an admin account cannot access the Data
  Stewardship console or its API endpoints; only `dataSteward` / `dataOwner` can.
- **Audit Trail** (`/portal/`, on any customer's detail view) — a read-only, append-only
  history of every golden record: its creation, every edit (manual portal edit, steward
  real-time reprocessing, or batch pipeline recompute/survivorship change), and logical
  deletes (a golden_id no longer produced by a pipeline rebuild — e.g. its sources were
  removed or it merged into a different golden_id). Opens as a separate panel from the
  customer detail modal, showing the record's key identifiers plus a newest-first
  timeline where each edit is one entry with expandable old→new field-level diffs.
  Viewable by any user with gold `read` or `read_write` access — same gating as viewing
  the record itself. There is no update/delete endpoint for it, by design.
- **Data Governance** (`/portal/`, a nav dropdown) — four items. Data Stewardship and
  Lineage and Impact Analysis are each independently role-gated; Reference Data
  Maintenance and Rules Configuration are visible to everyone (read-only outside
  dataSteward/dataOwner), so the dropdown itself is always shown:
  - **Data Stewardship** — visible only to `dataSteward`/`dataOwner` accounts (the
    same role gate as the Stewardship console itself). Opens `/app/` in a browser tab
    named `mdmStewardshipTab`; clicking it again while that tab is still open
    refocuses the same tab instead of opening a duplicate, since `window.open()`
    re-targets a tab by name rather than always creating a new one.
  - **Lineage and Impact Analysis** — visible to any user with gold `read` or
    `read_write` access (this only gates the portal's nav item; the underlying
    `/api/v1/lineage/*` endpoints remain open to any authenticated user regardless
    of `gold_access`, since the lineage graph is classified as pipeline metadata,
    not gold customer data — an intentional, pre-existing distinction). An
    explorable **network diagram** of the entire lineage metadata graph
    (bronze → silver → gold → stewardship), pannable and zoomable. Click any node
    to highlight its full upstream lineage (amber) and downstream impact (green)
    directly on the graph, and see details — description, direct incoming/outgoing
    rules, and upstream/downstream node counts — in a side panel. Clicking a
    gold-layer node also offers a quick lookup of any specific Golden ID's
    contributing source records.
  - **Reference Data Maintenance** — Country Codes and State Codes, each with a
    human-readable name/label and an active/deactivated status, in DB-native tables
    (`ref.ref_country_codes`/`ref.ref_state_codes`) the silver staging models read
    directly. Anyone can view the list; create/edit/deactivate is limited to
    `dataSteward`/`dataOwner` and goes through maker-checker (1 Data Owner approver).
  - **Rules Configuration** — Column Rules, Matching Rules (with their tier
    definitions), and **Survivorship Rules** (new — see below), all in DB-native
    tables (`bus_rules.*`) the batch pipeline and real-time reprocessing both read.
    Same read-everyone/write-steward-or-owner split as Reference Data Maintenance, but
    changes go through a stricter maker-checker gate (quorum of 2 different Data
    Owners) since these rules govern what the pipeline rejects, matches, and survives.
    Both screens, and both apps' Approvals views, render their tabs, columns, and
    forms entirely from a metadata catalog (`governance.entity_type_registry`, served
    at `GET /api/v1/governed-entities`) rather than hardcoded per-entity-type code —
    adding a new reference entity or rule category is a registry row plus a table, not
    a code change. A submitted `rule_type`/`match_method`/`transform_function` value
    with no real handler behind it is rejected outright at submission time instead of
    being silently accepted and never enforced — and for `column_rules.rule_type` and
    `survivorship_rules.rule_type`, that handler is now itself a registry (`api/
    validation.py`'s `RULE_TYPE_HANDLERS`, `api/reprocessing.py`'s
    `SURVIVORSHIP_RANK_HANDLERS`, and the dbt macros `rule_type_expr.sql`/
    `column_rule_flags.sql`), so a genuinely new rule type is one function or SQL
    branch to add, not edits scattered across the pipeline.
- **Maker-Checker Approval Workflow Engine** — several state-changing actions across
  both apps now submit into a generic, metadata-driven approval workflow
  (`api/workflow_engine.py`, `governance.workflow_definitions`) instead of applying
  immediately: stewardship remediation (resolve/reject an exception — 1 Data Owner),
  a Portal gold-record edit (a quorum of 2 different Data Owners), a Match Review
  confirm/reject (Data Owner, then Admin), a User Administration create or
  role/gold_access/is_active change (1 Admin, different from whoever requested it),
  a Reference Data Maintenance change (1 Data Owner), and a Rules Configuration change
  (a quorum of 2 different Data Owners). A maker can never approve their own
  submission, and the same approver can never decide twice on one instance. Both apps
  expose an **Approvals** view (backed by `GET /api/v1/workflows/pending`,
  `GET /api/v1/workflows/mine`, and `POST /api/v1/workflows/{instance_id}/decide`) for
  seeing what's awaiting a decision and what you've submitted — the same split-pane
  list/detail layout in both apps (a queue list on the left, "Awaiting my decision" and
  "My submissions" toggle plus a category filter above it, grouped by category then
  sorted by ID), and selecting a row shows who submitted it, then a read-only,
  boxed-and-framed view of the actual record the change affects. Exception and Match
  Review approvals show the exact same detail as their own console tabs; every
  workflow type highlights any field the submission changes (a darker green box, prior
  value shown below in red as `Was: …`), and a brand-new row (nothing to compare
  against yet) gets a "New" badge instead.
- **Pagination** — every list view in both apps (Exception Queue, Match Review,
  Approvals, Browse Customers, Audit Trail, Reference Data, Rules Configuration,
  User Administration) is paginated server-side: 10 rows per page by default,
  adjustable per-view to 25/50/100 via a selector next to the Prev/Next controls.
  The chosen page size is remembered in the browser (`localStorage`, per view) so
  it survives reloads and re-logins. Switching a filter, tab, or search query
  always returns you to page 1 of the new result set; if a page you're on gets
  filtered out from under you, the view silently corrects to the nearest valid
  page instead of showing a blank list. The Audit Trail paginates at the logical
  event level (one edit — however many fields it touched — is one page item),
  not the raw field-diff row level.
- **REST API** — exposes the gold layer (and its crosswalk) for downstream system
  consumption.

## Stack

- **dbt-core + DuckDB** for the pipeline (genuinely "dbt-style" — runs locally with
  zero cloud cost, no Databricks account needed)
- **scikit-learn** (TF-IDF + cosine similarity) for the fuzzy-matching tier — a
  lightweight, fully-local embedding technique (no model download, no GPU, no
  cloud API) chosen over a neural sentence-embedding model to keep setup fast
  and the demo runnable fully offline
- **Faker** to generate a larger, more realistic synthetic source population
- **FastAPI** for the app backends and REST API, with bcrypt-hashed password auth
- Plain HTML/JS frontends for both apps (no build step, no Node.js needed)
- **MCP (Model Context Protocol) + croniter** for an optional stateless agent/orchestration
  layer on top of the same pipeline — see "MCP Agent Orchestration" below

---

## Windows setup (PowerShell)

These steps assume you've extracted the zip into a folder such as:
`C:\Balram\Personal\Learning\AI\Claude\MDM Pipeline`

Open **PowerShell**, then:

```powershell
cd "C:\Balram\Personal\Learning\AI\Claude\MDM Pipeline"

python -m venv venv
venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

> If `python` isn't recognized, try `py` instead (the Python launcher), e.g. `py -m venv venv`.
>
> If PowerShell blocks the activation script with an execution-policy error, run
> `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` first, then retry.

### Build the pipeline

```powershell
# 1. Generate synthetic source data (CRM + ERP CSV extracts, ~100 rows each)
python data\generate_source_data.py

# 2. Land it in the bronze layer
python scripts\load_bronze.py

# 3. Build the rest of the pipeline (seed -> silver -> match/merge -> gold)
python scripts\build_pipeline.py
```

This is a **full load**: the first run for a given database, since
`bronze.load_control` (see below) has no history for CRM/ERP yet.

#### Incremental loads

`scripts/load_bronze.py` auto-detects full vs. incremental per source from
`bronze.load_control` (source_system, load_type, watermark_date,
last_loaded_at, row_count) -- no control row yet -> full load; a control row
already exists -> incremental, filtering the extract to records whose
`modified_date`/`last_updated` is on or after the stored watermark. Bronze
itself is append-only (every load lands new rows, never overwrites); the
silver staging models collapse it back down to one current row per key by
picking the latest `_load_ts`.

Every `load_bronze.py` run also logs one row to `bronze.load_batches`
(append-only -- `load_batch_id`, `load_type`, `sources[]`, `started_at`,
`row_count`), a single UUID shared by every row landed that run across both
sources. `stg_crm_customers.sql`/`stg_erp_customers.sql` are dbt
`incremental` models keyed on this: on a full-refresh (or first-ever) run
they scan bronze's entire history as before, but on a plain incremental run
they merge (`delete+insert` on `source_record_id`) only the rows stamped
with the most recent `load_batch_id` (looked up at compile time via
`macros/latest_load_batch.sql`) -- avoiding a rescan of bronze's full
history on every load. `build_pipeline.py` itself reads `bronze.load_batches`
before the silver `dbt run` to decide whether to pass `--full-refresh`.
Everything downstream of staging (`silver_all_staged.sql` onward, plus all
of gold) is still a full rebuild every run -- that's what lets deletion
propagation and match-group recompute work with no special-case code; see
`PROJECT_KNOWLEDGE.md` for the full design and its enterprise-scale caveat.

```powershell
# 3a. Generate an incremental batch (updates + logical deletes + new customers,
#     written on top of the persisted generator state in data\sources\_state\)
python data\generate_source_data.py --incremental

# 3b. Run the pipeline exactly as before -- load_bronze.py auto-detects
#     incremental mode and build_pipeline.py is unchanged either way
python scripts\load_bronze.py
python scripts\build_pipeline.py
```

Repeatable -- run 3a/3b again any time to simulate another load cycle.

Logical deletes: a deleted source record is filtered out of silver (and so
never reaches gold) on the next build. If it was the only active source
behind a golden record, that golden record disappears from
`gold_customers`/`gold_crosswalk` entirely on the next rebuild (captured as
`logically_deleted` in `audit.audit_trail`, not retained as a soft-deleted
row in the live table). If another source is still active for that golden
record, it re-survives from the remaining source(s) instead -- both happen
automatically via gold's existing full-rebuild-from-silver logic, no
gold-layer code is specific to deletes.

#### Forced full reload

Rebuilds from scratch even though load history already exists -- e.g. a demo
reset, or recovering from a bad incremental batch:

```powershell
python data\generate_source_data.py --full
python scripts\load_bronze.py --force-full
python scripts\build_pipeline.py
```

`--force-full` replaces bronze for both sources and resets their
`bronze.load_control` rows; it doesn't need to separately touch silver/gold --
the `bronze.load_batches` row this logs is `load_type='FULL'`, which is what
makes `build_pipeline.py` pass `--full-refresh` to the silver `dbt run`
(forcing `stg_crm_customers.sql`/`stg_erp_customers.sql` to rebuild from
bronze's entire history rather than merge just one batch), and gold and
everything else downstream of staging are still `table`-materialized dbt
models that already fully rebuild every run regardless, so they pick up the
reset automatically. `generate_source_data.py --full` (no state
yet) writes the deterministic seed=42 baseline; run again later (state
already exists) it dumps the *current* full state -- baseline plus every
incremental change since -- not a fresh re-roll. To truly reset the generator
back to the pure baseline, delete `data\sources\_state\generator_state.json`
first.

`build_pipeline.py` runs seven steps in order and stops if any of them fail:

```powershell
cd dbt_project
$env:DBT_PROFILES_DIR = "."
dbt --no-partial-parse seed
dbt --no-partial-parse run --exclude gold.*
cd ..
python scripts\cancel_stale_match_reviews.py  # cancels any open Match Review pair whose source data just changed
python scripts\generate_matches.py     # embedding-based match/merge -- needs silver, produces gold's input
cd dbt_project
dbt --no-partial-parse run --select gold.*
cd ..
python scripts\audit_pipeline_diff.py  # logs creates/updates/logical deletes to the gold-layer audit trail
```

`cancel_stale_match_reviews.py` has to run strictly between the silver rebuild and
`generate_matches.py`: it's what implements "an updated source record cancels the stale
Match Review pair it was on, before matching runs again" (see the Match Review hold note
below). While a pair's review is still open (pending, in review, or confirmed but not yet
actually merged), both of its silver records sit out of `generate_matches.py` entirely on
every run until the review resolves one way or the other.

If you ever rebuild just the gold layer by hand (`dbt run --select gold.*`, e.g. after a
Match Review confirm/reject) instead of via `build_pipeline.py`, run
`python scripts\audit_pipeline_diff.py` afterward too — otherwise that rebuild's changes
won't show up in the Audit Trail.

`--no-partial-parse` forces a full reparse instead of trusting dbt's
`target/partial_parse.msgpack` cache. Without it, a manifest cache left over
from a previous run (different machine, different OS, or just a moved/copied
project folder) can go stale and raise a `KeyError` looking up a macro file
that doesn't match the current parse. `build_pipeline.py` already passes this
flag; include it if you run the commands manually too.

The gold layer now depends on a Python step (`scripts/generate_matches.py`,
TF-IDF embedding similarity via scikit-learn) that has to run strictly between
silver and gold, so a single `dbt run` can no longer build the whole pipeline
end to end — run `build_pipeline.py`, or the four commands above, instead.
If you only changed something silver-or-earlier and don't need to re-match,
`dbt run --exclude gold.*` alone is enough; if you only changed a steward's
match-review decision, `python scripts\generate_matches.py` followed by
`dbt run --select gold.*` is enough.

This produces `mdm_demo.duckdb` in the project root — a single-file database
containing every layer (bronze, silver, gold, rules metadata, lineage metadata,
plus the portal's user/session tables once you complete the next step).

### Create the admin user

```powershell
python scripts\create_admin_user.py
```

This prints a **one-time password** for the `mdm_admin` account to your terminal —
copy it now, it is never shown again (and never stored in plaintext; only its
bcrypt hash is saved). If you ever get locked out, run
`python scripts\reset_admin_password.py` to generate a new one.

### Create demo governance accounts (optional, for testing approvals)

```powershell
python scripts\create_demo_governance_users.py
```

The maker-checker workflows above need a *different* person than the requester to
approve (a Data Owner quorum needs 2 different Data Owners, a User Administration
change needs a second Admin) — this project ships one account per role by default,
which isn't enough on its own. This prints one-time passwords for `mdm_dataowner2`,
`mdm_dataowner3`, and `mdm_admin2`. Safe to re-run; existing accounts are skipped.

### Run the app + API

```powershell
cd api
uvicorn main:app --reload --port 8000
```

Then open in your browser:
- **MDM Data Hub Portal (login required):** http://localhost:8000/portal/
  — sign in with `mdm_admin` and the password from the step above.
- **Data Stewardship console:** http://localhost:8000/app/
- **REST API docs (auto-generated):** http://localhost:8000/docs
- **Health check:** http://localhost:8000/health

### Enabling real AI-assisted remediation (optional)

```powershell
$env:ANTHROPIC_API_KEY = "your_key_here"
```
Set this before starting `uvicorn`. Without it, the stewardship app uses a
transparent heuristic fallback so the demo runs fully offline — every suggestion
response includes a `source` field (`"ai"` or `"heuristic_fallback"`) so it's always
clear which path produced it.

---

## macOS / Linux setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

python3 data/generate_source_data.py
python3 scripts/load_bronze.py
python3 scripts/build_pipeline.py      # seed -> silver -> match/merge -> gold

# Incremental load (repeatable): python3 data/generate_source_data.py --incremental
#                                 then load_bronze.py + build_pipeline.py again
# Forced full reload:             python3 data/generate_source_data.py --full
#                                 python3 scripts/load_bronze.py --force-full
#                                 python3 scripts/build_pipeline.py
# See "Build the pipeline" above (Windows section) for the full explanation --
# same scripts/flags, just python3 instead of python.

python3 scripts/create_admin_user.py   # copy the printed one-time password
python3 scripts/create_demo_governance_users.py   # optional: extra accounts to test maker-checker approvals

cd api
uvicorn main:app --reload --port 8000
```
Enable AI remediation with `export ANTHROPIC_API_KEY=your_key_here` before starting `uvicorn`.

---

## Key REST API endpoints

| Endpoint | Purpose |
|---|---|
| `POST /api/v1/auth/login` | Authenticate, returns a session token |
| `GET /api/v1/auth/me` | Current user's identity/role/access |
| `GET /api/v1/customers` | List golden customer records (auth required; supports `q=` search, plus `state_code`/`country_code`/`source_system_count`/`confidence_tier`/`survivor_source_system`/`pending_approval` filters) |
| `GET /api/v1/portal/dashboard/customers-summary` | Customer Dashboard aggregate: source count, confidence tier, survivor source, geography, pending-approval, activity trend |
| `GET /api/v1/customers/{golden_id}` | Get one golden record |
| `PUT /api/v1/customers/{golden_id}` | Submit a golden-record edit (requires `read_write` gold access; applies once a 2-Data-Owner quorum approves) |
| `GET /api/v1/customers/{golden_id}/sources` | Crosswalk: contributing source records |
| `GET /api/v1/customers/{golden_id}/audit-trail` | Read-only history: creation, edits, logical deletes (any gold `read`/`read_write` access) |
| `GET /api/v1/lineage/impact?layer=&table=&column=` | Forward impact analysis |
| `GET /api/v1/lineage/trace?layer=&table=&column=` | Backward lineage trace |
| `GET /api/v1/lineage/record/{golden_id}` | Full lineage for one golden record |
| `GET /api/v1/admin/users` | List portal users (admin role required) |
| `POST /api/v1/admin/users` | Submit a new-user request (admin role required; a different admin must approve) |
| `PUT /api/v1/admin/users/{user_id}` | Update `full_name` immediately; `role`/`gold_access`/`is_active` changes submit for a different admin's approval |
| `POST /api/v1/admin/users/{user_id}/reset_password` | Reset a user's password (admin role required; not gated by an approval) |
| `GET /api/v1/stewardship/queue?status=open` | Exception queue (also accepts `status=all`, `reason=`, or `activity=` for dashboard drill-throughs) |
| `GET /api/v1/stewardship/dashboard/exceptions-summary` | Exception Dashboard aggregate: status/reason breakdown, created-vs-resolved-vs-rejected trend, caller's own activity |
| `POST /api/v1/stewardship/queue/{id}/suggest` | Get AI/heuristic remediation suggestion |
| `POST /api/v1/stewardship/queue/{id}/resolve` | Submit a correction for Data Owner approval (reprocesses once approved) |
| `POST /api/v1/stewardship/queue/{id}/reject` | Submit a rejection for Data Owner approval |
| `GET /api/v1/stewardship/match-review?status=pending` | Borderline fuzzy-match candidates (dataSteward/dataOwner role required; also accepts `status=all`, `similarity_bucket=`, or `activity=` for dashboard drill-throughs) |
| `GET /api/v1/stewardship/dashboard/match-review-summary` | Match Review Dashboard aggregate: status breakdown, similarity-score buckets, caller's own activity |
| `GET /api/v1/stewardship/match-review/{pair_id}` | One match-review candidate pair, full side-by-side detail |
| `POST /api/v1/stewardship/match-review/{pair_id}/confirm` | Submit a confirm decision for approval (Data Owner, then Admin) |
| `POST /api/v1/stewardship/match-review/{pair_id}/reject` | Submit a reject decision for approval (Data Owner, then Admin) |
| `GET /api/v1/workflows/pending` | Workflow instances awaiting the caller's decision |
| `GET /api/v1/workflows/mine` | Workflow instances the caller submitted, with status |
| `GET /api/v1/workflows/{instance_id}` | One workflow instance, full detail including decisions so far |
| `POST /api/v1/workflows/{instance_id}/decide` | Approve or reject the current step (`{decision, comment}`) |
| `GET /api/v1/reference-data/{entity_type}` | List country codes or state codes (`ref_country_code`/`ref_state_code`) — any authenticated user |
| `POST /api/v1/reference-data` | Submit a create/update/deactivate for Data Owner approval (dataSteward/dataOwner only) |
| `GET /api/v1/rules-config/{entity_type}` | List column rules, matching rules, matching thresholds/tiers, or survivorship rules — any authenticated user |
| `POST /api/v1/rules-config` | Submit a create/update/deactivate for a 2-Data-Owner quorum (dataSteward/dataOwner only) |

Every `GET` endpoint above that returns a list also accepts `limit`/`offset` (default
`limit=10`, matching the UI's default page size) and echoes `total`/`limit`/`offset`
alongside the page of `items` (`/customers` calls its list field `customers`,
`/admin/users` calls it `users` — every other list endpoint uses `items`; the
`count` field on `/stewardship/queue` and `/stewardship/match-review` is an alias
for `total` kept for backward compatibility). `/customers/{golden_id}/audit-trail`
paginates at the logical-event level (`change_batch_id`), so `limit=10` means 10
events, not 10 raw field-diff rows — an edit touching 5 fields is still one page item.

All `/api/v1/customers*`, `/api/v1/admin/*` and `/api/v1/auth/me` endpoints require an
`Authorization: Bearer <token>` header from `/api/v1/auth/login`.

## MCP Agent Orchestration (alternative to `build_pipeline.py`)

The same five pipeline steps are also available as stateless agents (`agents/`) coordinated
by an MCP server (`mcp_server/`) acting as a central orchestrator and tool registry. This is
additive — `scripts/build_pipeline.py` still works exactly as documented above and is
unaffected. See `MCP_Agent_Orchestration_Design.docx` for the full design and verification
details.

```powershell
pip install -r requirements.txt   # now also installs mcp>=1.9,<2.0 and croniter

# One-off / ad hoc run of the full sequence (Ingestion -> DQ -> Matching -> Survivorship -> Steward):
python -m agents.orchestration_agent --run-now

# Metadata-driven scheduler: reads governance.orchestration_schedule (default: nightly
# 02:00 UTC) and only actually runs the pipeline when a schedule's cron fires. The OS
# scheduler just needs to invoke this frequently -- the real cadence lives in the DB row:
python -m agents.orchestration_agent --serve --poll-interval 900

# MCP server (tool registry) -- for an external MCP client, e.g. Claude Desktop/Code:
python -m mcp_server.server
```

Each agent can also be run standalone for testing, e.g. `python -m agents.monitoring_agent`
or `python -m agents.dq_agent`. Every run of the Orchestration Agent is logged, append-only,
to `governance.orchestration_run_log`; the Monitoring Agent (`monitoring.check_health` /
`monitoring.get_metrics` MCP tools) reads that log plus the exception/Match Review/approval
queues to report overall pipeline health graded against `governance.monitoring_thresholds`.

## Design notes / known simplifications (by design, for demo scope)

- **Matching is metadata-driven**, the same pattern as `bus_rules.column_rules`: tier
  definitions and thresholds live in `bus_rules.matching_thresholds`,
  and the fields each tier compares live in `bus_rules.matching_rules`
  (a child table keyed by tier_id) — both DB-native, maintained via the Data Governance >
  Rules Configuration screen. Nothing about *which* fields matter or *what*
  threshold counts as a match is hardcoded in `scripts/generate_matches.py` or
  `api/reprocessing.py` — both read the table at runtime. Today's configuration has a
  hybrid of deterministic exact matching (normalized email or phone, confidence =
  that tier's `auto_merge_threshold`) and an embedding-similarity fuzzy tier (TF-IDF
  character n-gram cosine similarity over name/address text, blocked by
  state_code). Fuzzy matches above the configured `auto_merge_threshold` auto-merge with
  their similarity score as confidence; matches between `review_lower_threshold` and
  `auto_merge_threshold` go to a **Match Review** queue for a steward to confirm or
  reject rather than auto-merging. This is a real embedding-similarity technique used
  in production dedup systems, deliberately kept lightweight (no model download,
  fully offline, no GPU) rather than a neural sentence-embedding model — a
  reasonable next iteration if higher recall on more subtle near-duplicates is
  needed. See `scripts/generate_matches.py` for the full algorithm and calibration
  notes, and `bus_rules.matching_thresholds` / `bus_rules.matching_rules` for the
  tier/field configuration itself.
  **Known gap:** the real-time reprocessing path (steward resolves a
  validation exception → immediate re-match) still only evaluates the exact
  (`match_method='exact'`) tier; the fuzzy tier only runs in the batch pipeline.
  Fitting a TF-IDF vectorizer per API request was judged not worth the added
  request latency for a demo-scoped feature. Both paths read the same
  `exact_match_field` rules, though, so they can't disagree on which fields
  count as an exact match.
- **Match Review hold**: while a Match Review pair's review is open (`pending`/
  `in_review`, or the rare case of `confirmed` but not yet actually merged — e.g. the
  real-time merge step itself failed after approval, see the maker-checker note below),
  both of its silver records sit out of `generate_matches.py` entirely on every subsequent run, so they can't be auto-merged
  via a different edge or drawn into an unrelated new suspect match while their identity
  is still undecided. If an updated version of either side's source record lands in
  silver while the review is open, the review is auto-cancelled with an explanatory note
  (`stewardship.match_review_overrides.status = 'cancelled'`) rather than left to keep
  judging stale data — triggered by `api/reprocessing.py` in real time and
  `scripts/cancel_stale_match_reviews.py` in the batch pipeline.
- **Survivorship is attribute-level**: each gold column is picked independently by its
  own rule in `bus_rules.survivorship_rules` (most common / most complete / oldest /
  newest / pattern match across contributing sources), tie-broken by most recently
  modified (then CRM preferred) — see `dbt_project/models/gold/gold_survivorship_winners.sql`
  for the batch evaluation and `api/reprocessing.py`'s `_pick_column_winners()` for the
  real-time equivalent. `gold_crosswalk`'s `winning_columns` shows exactly which gold
  columns each contributing source won. Known gap: no cross-column consistency check
  (see `PROJECT_KNOWLEDGE.md`).
- Rules are documented as metadata (`bus_rules.column_rules`) and referenced by rule ID
  throughout the pipeline and stewardship app; the current implementation applies
  them via explicit SQL rather than fully dynamic rule interpretation, trading a
  bit of "purity" for time-boxed reliability.
- **Auth** is a from-scratch bcrypt + bearer-token implementation sized for a demo
  (sessions expire after 8 hours, no refresh tokens, no MFA). A production system
  would typically delegate this to an identity provider (Okta, Azure AD, etc.).
- The **maker-checker workflow engine** gates six workflows (see above). If the
  underlying action fails after every required
  approval is already recorded, the instance is still marked `approved` (the human
  decision stands) with the failure captured in its `result` field rather than
  silently rolled back — there's no automatic retry. A quorum or multi-level chain
  also needs enough distinct people in the required role to ever clear; see
  `scripts/create_demo_governance_users.py`.
- The database (`mdm_demo.duckdb`) is a single local file — fine for a demo, not a
  substitute for a real concurrent multi-user warehouse.
- **Steward-corrected records ARE re-run through the reject-severity validation
  rules** (`api/validation.py`, mirroring `bus_rules.column_rules`) before reprocessing.
  If the correction still fails validation, the steward sees an alert explaining
  what's still wrong and the record stays in the exception queue — it is not
  marked resolved and does not reach match/merge. Only once a correction
  genuinely passes validation does it flow through, at which point the "modified"
  timestamp is stamped as the moment of correction, so under a `newest`-rule gold
  column (the default for every column until a governance user tunes it) a fresh
  correction will typically become the new winner for that column.
- The real-time reprocessing match step compares against each existing golden
  record's *current* email/phone (whichever source currently wins those specific
  columns), not every historical source in that group.
- Golden IDs created via real-time reprocessing are numbered by incrementing the
  current max, which can diverge from what a full `dbt run` from scratch would
  assign (dbt's match-group numbering is order-dependent on the complete silver
  set). Both paths stay internally consistent; the ID numbers themselves aren't
  guaranteed portable between a live-reprocessed record and a full pipeline rerun.
- **Audit Trail** (`api/audit.py`, `scripts/audit_pipeline_diff.py`) is append-only by
  construction — no code path anywhere issues an UPDATE or DELETE against it, and there's
  no write/delete endpoint. It inherits the golden-ID-renumbering caveat above: if a
  reprocessing-created golden_id gets renumbered by the next full `dbt run`, the trail
  shows the old number as logically deleted and the new number as created, even though
  it's the same customer. It also only captures a manual `dbt run --select gold.*` if
  `audit_pipeline_diff.py` is run immediately after (automatic inside `build_pipeline.py`).
