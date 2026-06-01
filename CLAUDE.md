# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Nessie (Networked Engines Supply Statistics in Education) is a UC Berkeley educational data platform that aggregates student data from external systems (Canvas, CalNet, EDL, Redshift, etc.) via background jobs and surfaces it through a Flask API and Vue 3 frontend. The RDS databases also serve as an API layer queryable by external applications.

## Commands

### Python / Backend

```bash
tox                            # Run all tests and linters
tox -e test                    # Run pytest only
tox -e test -- tests/test_api/test_admin_controller.py  # Single test file
tox -e test -- tests/test_api/test_admin_controller.py::test_function_name  # Single test
tox -e lint-py                 # Ruff linting
```

External integration tests (require live service credentials):
```bash
NESSIE_ENV=testext pytest      # Tests marked @pytest.mark.testext
```

### Vue / Frontend

```bash
npm run serve-vue              # Dev server with hot reload at localhost:8080
npm run build-vue              # Build frontend to dist/static
npm run lint-vue               # ESLint check
npm run lint-vue-fix           # Auto-fix ESLint issues
```

### Running Locally

```bash
# Terminal 1: Flask backend at http://localhost:5001
python3 application.py

# Terminal 2: Vue dev server at http://localhost:8080
npm run serve-vue
```

## Architecture

### Configuration Loading

`nessie/configs.py` loads configs in this order:
1. `config/default.py` — base defaults
2. `config/{NESSIE_ENV}.py` — environment override (`development`, `test`, `testext`)
3. `{NESSIE_LOCAL_CONFIGS}/{NESSIE_ENV}-local.py` — local secrets/credentials (not committed)

### Backend Structure

**`application.py`** → `nessie/factory.py` (Flask app factory) → `nessie/routes.py` (registers blueprints)

**`nessie/api/`** — Flask controllers for HTTP endpoints:
- `job_controller.py` — trigger/query background jobs
- `schedule_controller.py` — job scheduling management
- `admin_controller.py` — admin operations
- `auth_helper.py` — CAS authentication

**`nessie/jobs/`** — 60+ background job classes (all extend `BackgroundJob`). Jobs create or refresh database schemas by pulling data from external systems into Redshift/RDS. Execution is either scheduled via APScheduler (`lib/scheduling.py`) or queued via SQS (`lib/queue.py`). The deployment distinction between "highlands" (scheduler) and "lowlands" (worker) instances is set via `WORKER` config flag.

**`nessie/externals/`** — Clients for every external system: AWS services (S3, Redshift, RDS, Lambda, Secrets Manager), Canvas, CalNet, EDL, BOAC, and others.

**`nessie/lib/`** — Shared utilities. `metadata.py` tracks job run status in RDS. `util.py` handles SQL template resolution. `berkeley.py` contains UC Berkeley–specific logic.

**`nessie/merged/`** — Logic to combine data from multiple sources into unified student views (demographics, terms, SIS profiles).

**`nessie/sql_templates/`** — 50+ Jinja-style SQL templates rendered at runtime. Changes here affect what data gets written to Redshift.

### Databases

- **RDS (PostgreSQL):** Metadata, job tracking, admin data, BI reporting
- **Redshift:** Large-scale student data warehouse backed by S3 external tables

In many cases, the RDS table structure mirrors the underlying Redshift structure, and many Nessie jobs copy data from Redshift to RDS for consumption by external applications.

Test environments mock Redshift behavior using PostgreSQL (see `tests/conftest.py` fixtures: `student_tables`, `sis_note_tables`).

### Frontend Structure (`src/`)

Vue 3 + TypeScript with Vuetify 3 UI, Pinia state management, and Axios for API calls. Built with Vite.

Key views:
- `MagicEightBall.vue` — project planning interface somewhat separate from the rest of Nessie
- `JobTable.vue` — job history/status display and ad hoc execution
- `Schedule.vue` — job scheduling management
- `Configs.vue` — configuration display

State lives in `src/stores/context.ts`. API calls go through `src/api/`.

### Testing

Tests in `tests/` mirror the `nessie/` package structure (`test_api/`, `test_jobs/`, `test_externals/`, `test_lib/`, `test_merged/`). The session-scoped `app` fixture in `conftest.py` creates one Flask app for the entire test run. Tests marked `@pytest.mark.testext` require live external service connections and are excluded from normal `tox -e test` runs.
