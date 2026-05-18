## Project process / architecture guide (detailed)

This document is the “single source of truth” for understanding the project:

- **What the product does**
- **How data flows** through the UI, API proxy, backend, and reporting
- **How the guided chat works** (why it is deterministic in key places)
- **How the Data Pipeline wizard works**
- **What each major file/module exists for** (and what would break if removed)
- **Why the report HTML theme is reused in the UI**
- **Operational notes**: session behavior, safety limits, outputs, and common failure points

---

## 1) What the project is

This repo contains:

- **Frontend**: a Next.js (App Router) UI that provides:
  - A **Chat** experience (guided selection: choose source → choose tables/files → view rows/schema/metadata → generate report).
  - A **Data Pipeline** experience (wizard-like flow: select source → select files → assess → report → requirements → ETL → cleaning).
- **Backend**: a FastAPI service (“MCP server”) that:
  - Connects to data sources (Azure SQL, Azure Blob, local filesystem).
  - Performs data profiling + data quality checks + cross-dataset relationship detection.
  - Generates **one JSON + one MD + one HTML** report and overwrites them each run.
  - Returns results to the frontend for rendering (including a themed HTML report UI).

### What “assessment” means in this project

When the user runs an assessment (via chat or the pipeline), the backend:

- Loads a **bounded sample** of data from each selected dataset (bounded by safety env vars).
- Builds a **profile** per dataset:
  - row/column counts (for the sampled set)
  - per-column stats (null %, unique count, inferred semantic type, PK candidacy, etc.)
- Runs **data quality checks**:
  - missing values, invalid patterns, duplicates, mixed types, etc.
- Detects **relationships** between datasets:
  - shared key overlap, inferred cardinality (1:1, 1:N, M:N), warnings
- Computes **global issues**:
  - orphan foreign keys, relationship row issues, cross-dataset inconsistencies

The result is stored in the backend session for follow‑up questions and is also exported to files.

---

## 2) High-level architecture (runtime)

### Frontend runtime (Next.js)

- The browser UI calls Next.js API routes under `app/api/*`.
- Those API routes **proxy** requests to the backend FastAPI server.
- The UI persists a lightweight “session id” in browser storage:
  - `localStorage.dharaSessionId` → identifies a conversation/session on the backend.

### Backend is the “source of truth”

The frontend UI is intentionally thin for complex reporting:

- The backend generates the canonical report UI as **HTML** (`report.html`).
- The frontend embeds that HTML (and other themed HTML pages) via an iframe `srcDoc`.

This ensures the UI looks identical to the exported HTML report and prevents duplicated logic.

### Backend runtime (FastAPI)

- The backend runs a FastAPI server defined in:
  - `Agent Dhara Backend/agent/mcp_server.py`
- It exposes endpoints such as:
  - `/chat`: guided chat workflow (tables/files selection, view schema/metadata/rows, generate report)
  - `/list_tables`: list SQL tables using config
  - `/sources`: list configured source locations
  - `/sessions/*`: session persistence and retrieval
  - `/upload`: upload file → run assessment

### Request/Response routing overview

Frontend calls:
- `POST /api/chat` → Next.js route → backend `POST /chat`
- `POST /api/list-tables` → Next.js route → backend `POST /list_tables`
- `GET /api/sources` → Next.js route → backend `GET /sources`
- `POST /api/session-context` → Next.js route → backend `POST /sessions/context`

Backend responds with:
- `reply`: human-readable content (Markdown or plain text)
- `payload`: structured objects for the UI (tables/files/options + report_html/ui_html + result JSON)

### Report generation

Backend generates 3 report artifacts for the latest run and **overwrites** them each time:

- `Agent Dhara Backend/output/reports/report.json`
- `Agent Dhara Backend/output/reports/report.md`
- `Agent Dhara Backend/output/reports/report.html`

The backend also returns:
- `payload.result`: full structured assessment JSON
- `payload.report_markdown`: markdown report (tabular)
- `payload.report_html`: full themed HTML report (executive UI)

The frontend renders the HTML report in an iframe so the UI matches the report theme exactly.

#### Overwrite behavior (important)

The backend overwrites the 3 report files **every time** you generate a report. This ensures:

- There is always a single “latest report” for downstream steps
- You do not accumulate stale timestamped reports that confuse users

---

## 3) Configuration flow (sources / credentials)

### Source configuration file

The data sources are defined in a YAML config (typically):

- `Agent Dhara Backend/config/sources.yaml`

The config contains `locations` such as:
- `type: database` (Azure SQL)
- `type: azure_blob` (Blob container)
- `type: filesystem` (local path)

The frontend can call `/api/sources` which proxies to backend `/sources` to discover available locations.

### Environment variables

Backend environment variables are loaded from:
- `Agent Dhara Backend/.env` (developer machine; should not be committed)

Important “safety limit” knobs:
- `ASSESS_MAX_BLOB_BYTES`
- `ASSESS_MAX_ROWS_PER_BLOB`
- `ASSESS_MAX_ROWS_PER_TABLE`
- `ASSESS_MAX_ROWS_PER_LOCAL_FILE`

They cap how much data is loaded during assessment to prevent timeouts / memory spikes.

### Why the safety limits matter

Profiling and DQ checks are pandas-heavy and can blow up in:

- RAM usage (wide tables, text columns, high cardinality)
- Runtime (large row counts, expensive relationship inference)
- Network timeouts (Azure SQL / Blob)

So the “max rows / max bytes” limits exist to keep the system responsive and prevent runaway costs.

---

## 4) Data assessment engine (what it computes)

The assessment pipeline is implemented in:

- `Agent Dhara Backend/agent/intelligent_data_assessment.py`

It outputs a single JSON object that contains:

- **datasets**: per dataset profile:
  - `row_count`, `column_count`, `columns{...}`, `source_root`, etc.
- **relationships**: cross-dataset overlaps:
  - dataset/column pairs, overlap counts, inferred cardinality
- **data_quality_issues**
  - `datasets`: issues per dataset + severity rollups
  - `global_issues`: relationship row issues, orphan keys, warnings, cross-dataset inconsistencies, etc.

This output is the “source of truth” for UI and report files.

### Result shape (practical)

The backend assessment JSON is conceptually:

- `datasets[name]`:
  - `row_count`, `column_count`
  - `source_root` (where it came from: `__database__:*`, `azure_blob:*`, filesystem path)
  - `columns[col]` with dtype/null%/unique/semantic/PK candidate
- `data_quality_issues.datasets[name]`:
  - `summary` totals: high/medium/low
  - `issues[]` rows: severity/type/column/count/message/recommendation
- `relationships[]`:
  - `dataset_a/column_a` ↔ `dataset_b/column_b`
  - overlap counts and inferred cardinality
- `data_quality_issues.global_issues`:
  - orphan keys, relationship row issues, warnings, inconsistencies

---

## 5) Chat workflow (guided flow)

### Core backend file

The chat flow and its deterministic routing lives in:

- `Agent Dhara Backend/agent/chat_graph.py`

It implements:
- **Routing** (`_node_route`):
  - Handles “select tables … / select all tables / show schema / show metadata / preview / generate report” deterministically.
  - This avoids relying on an LLM to parse selection commands (LLMs can drop indices).
- **Selection nodes**:
  - list sources → select source
  - list tables/files → select tables/files
- **View nodes**:
  - schema
  - metadata
  - preview rows (paged)
- **Report nodes**:
  - run assessment on selected tables/files
  - generate `report.json/.md/.html` and return `report_html` to UI

### Deterministic routing: why and how

Some commands must be exact:
- selecting tables by index
- selecting all tables/files

An LLM can “helpfully” rewrite or drop numbers, so the backend router (`_node_route`) intercepts
selection commands such as:

- `select all tables`
- `select tables 1,2,3`
- `select files 1 2 3`
- `select local files 1;2;3`

…and routes them directly to selection handlers without the LLM.

This makes **Select all** reliable even with long lists.

### Frontend chat UI

The chat UI rendering is in:

- `components/ChatWindow.tsx`

Key points:
- It reads the session id from `localStorage.dharaSessionId`.
- It calls `/api/chat`, which proxies to backend `/chat`.
- When backend returns `payload.report_html` (or `payload.ui_html` for schema/metadata/rows),
  the UI renders it in an iframe (`srcDoc`) so it matches the backend theme.
- “New chat” generates a new `dharaSessionId` and resets UI state.

### “New chat” behavior (important)

When the user clicks **New chat**:

- The UI generates a new `dharaSessionId`
- Clears the agent thread id and selected data source
- Fires a `dhara-session-change` event
- The chat component resets its state and loads messages for the new session
- If no messages exist for that session, a greeting is seeded

### Themed pages for schema/metadata/rows

When you click “Show schema / Show metadata / View rows” in chat:
- Backend returns `payload.ui_html` (theme-wrapped HTML).
- Frontend shows it in the same iframe style as the report.

#### What is inside `ui_html`

`ui_html` is not a separate frontend UI. It is server-generated HTML that:

- imports the same Theme 2 CSS as the report
- renders tables for schema/metadata/rows using the same styling
- keeps the UI consistent across “Report”, “Schema”, “Metadata”, and “Preview”

---

## 6) Data Pipeline workflow (wizard flow)

The wizard page is:

- `app/data-pipeline/page.tsx`

It implements steps like:
- Database selection
- File/table selection
- Assessment run
- Report step (JSON/MD/HTML view + download)
- Requirements capture
- ETL generation
- Cleaning

The assessment step UI uses:
- `components/DataAssessmentReport.tsx`

It calls:
- `/api/chat` with commands like “assess selected tables/files”
after storing deterministic session context via `/api/session-context`.

### Step-by-step pipeline flow (practical)

1. **Database selection**
   - UI lists configured sources
   - Stores a token that describes which backend location is selected
2. **File/table selection**
   - SQL: calls `/api/list-tables`
   - Blob/local: calls `/api/chat` → list files
3. **Assessment**
   - UI pushes selection context into the backend session
   - UI sends a deterministic command like “assess selected tables”
4. **Report**
   - UI can download JSON / MD / HTML (HTML is the themed UI)
5. **Requirements**
   - User provides business requirements used for ETL generation
6. **ETL / Cleaning**
   - Uses assessment outputs as the input context

---

## 7) Why specific files exist (important ones)

### Backend

- `Agent Dhara Backend/agent/mcp_server.py`
  - FastAPI server entrypoint (auth, CORS, endpoints).
- `Agent Dhara Backend/agent/chat_graph.py`
  - Chat workflow graph, deterministic router, report generation, themed HTML for UI.
- `Agent Dhara Backend/agent/intelligent_data_assessment.py`
  - Core profiling, DQ rules, relationships, global issues.
- `Agent Dhara Backend/main.py`
  - CLI runner + report builders:
    - `build_markdown_report(result)`
    - `build_html_report(result)` (Theme 2 “executive UI”)
- `Agent Dhara Backend/agent/report_html_themes.py` + `Agent Dhara Backend/agent/report_themes/theme2.css`
  - The HTML theme used by `report.html` (CSS source of the UI look).

### Why some outputs are created (files/dirs)

- `Agent Dhara Backend/output/reports/report.*`
  - Persisted artifacts so the “latest report” can be opened independently of chat history.
  - Useful for pipeline steps and auditability.

### Frontend

- `components/ChatWindow.tsx`
  - Main chat UI + interactive selection + iframe rendering for themed HTML.
- `components/Sidebar.tsx`
  - “New chat” and navigation, sets new session ids.
- `app/api/chat/route.ts`
  - Next.js API route that proxies chat to backend.
- `app/api/list-tables/route.ts`, `app/api/sources/route.ts`, etc.
  - Proxy routes to backend endpoints.

### What not to delete (key invariants)

If you delete these, core functionality breaks:

- Backend: `agent/mcp_server.py`, `agent/chat_graph.py`, `agent/intelligent_data_assessment.py`, `main.py`
- Frontend: `components/ChatWindow.tsx`, `app/api/chat/route.ts`, `app/chat/page.tsx` (entry page)
- Theme: `agent/report_themes/theme2.css` (HTML report styling)

---

## 8) Implementation decisions (the “why”)

### Why the backend emits HTML and the frontend embeds it

The themed HTML report (`report.html`) already contains:
- The complete UX (sections, tables, navigation, styles, expand/collapse, etc.)

Embedding it in the UI ensures:
- The UI looks exactly like the report
- The frontend doesn’t need to duplicate complex layout logic
- Future theme improvements happen in one place (backend theme)

### Why selections are handled deterministically

Selection commands like “select tables 1,2,3” must be exact.
LLMs can occasionally omit numbers or re-order them, so the backend router:
- Parses selection commands directly
- Routes to `select_tables` / `select_blob_files` / `select_local_files`
without the LLM

This makes “Select all” reliable.+

### Why HTML is embedded with iframe `srcDoc`

This approach:

- avoids rebuilding the theme in React
- ensures pixel-identical parity between “exported report” and “in-app report”
- keeps complex interactive features (expand/collapse, navigation rail) within the report HTML

Trade-offs:

- HTML runs inside an iframe (isolated DOM)
- some browser restrictions apply, so sandbox flags are used carefully

---

## 9) Running the project (dev)

Typical dev setup:

- Start backend (FastAPI) from `Agent Dhara Backend/`:
  - `uvicorn agent.mcp_server:app --host 127.0.0.1 --port 8008 --env-file .env`
- Start frontend (Next.js) from repo root:
  - `npm run dev`

Then open:
- Frontend: `http://localhost:3000`

### Common issues

- **Azure SQL firewall**: you may see “Client IP not allowed” if the server blocks your IP.
  - Fix by adding your IP to Azure SQL firewall rules.
- **Large datasets**: increase safety limits carefully (rows first, then bytes) if you need more coverage.
- **Stale session context**: use “New chat” to start clean if the workflow seems stuck.

---

## 10) Cleaning / repo hygiene

This repo previously had many `.md` documentation files.
They were removed to reduce clutter and keep only “code that runs”.

Build outputs are ignored by `.gitignore` (e.g. `.next/`), so they should not be committed.

