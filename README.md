# Data Engineering Job Posting Pipeline

A medallion-architecture pipeline that pulls US Data Engineer listings from [JSearch](https://rapidapi.com/letscrape-6bRBa3QguO5/api/jsearch) (RapidAPI), stores and transforms them on Databricks (Delta Lake), and surfaces market trends in a Streamlit dashboard.

## Architecture

```
JSearch API
    │
    ▼
ingest_postings.py  ──►  Unity Catalog Volume
                         workspace/bronze/raw_data/postings_<timestamp>.json
    │
    ▼
bronze_to_silver.py  ──►  workspace.silver.job_postings
    │                     flatten · dedupe · merge on job_id
    │                     extract skills, min years, education
    ▼
silver_to_gold.py    ──►  workspace.gold.skill_demand
                          workspace.gold.experience_requirements
                          workspace.gold.location_salary_trend
    │
    ▼
dashboard/app.py     ──►  Streamlit (Databricks SQL warehouse)
```

| Layer | Location | What it holds |
| --- | --- | --- |
| Bronze | Volume `workspace.bronze.raw_data` | Raw API JSON, one file per ingest |
| Silver | `workspace.silver.job_postings` | Flattened postings, upserted by `job_id` |
| Gold | `workspace.gold.*` | Aggregates for the dashboard (overwrite each run) |

## Repository layout

```
src/
  ingestion/
    jsearch_client.py      RapidAPI client (key from Databricks secrets)
    ingest_postings.py     Fetch today's DE jobs and upload to the bronze volume
  transformation/
    keyword_extractor.py   Regex extractors: skills, years of experience, education
    bronze_to_silver.py    JSON → silver Delta table (merge)
    silver_to_gold.py      Aggregations → gold tables
  dashboard/
    app.py                 Streamlit app over gold tables
```

## Prerequisites

- Python **3.12**
- [uv](https://docs.astral.sh/uv/) (this project uses `pyproject.toml` + `uv.lock`)
- A Databricks workspace with:
  - Unity Catalog schemas `bronze`, `silver`, and `gold` under catalog `workspace`
  - Volume `workspace.bronze.raw_data`
  - Secret scope `rapidapi` with key `jsearch_key`
  - A SQL warehouse (for the dashboard)
- Databricks Connect **~18.0** (declared in the `dev` dependency group)

Local setup is managed by Databricks environments (`databricks environments setup-local`). Constraints in `pyproject.toml` under `[tool.uv]` are generated — do not edit them by hand.

## Configuration

### Ingestion (Databricks secrets)

`jsearch_client.py` reads the RapidAPI key at runtime:

- Scope: `rapidapi`
- Key: `jsearch_key`

The search is fixed to:

- Country: `us`
- Posted: `today`
- Query: `data engineer jobs`

### Dashboard (Streamlit secrets)

Create `.streamlit/secrets.toml` (gitignored) with:

```toml
DATABRICKS_HOST = "your-workspace.cloud.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/<warehouse-id>"
DATABRICKS_TOKEN = "<personal-access-token>"
```

## How the stages work

### 1. Ingest (bronze)

`src/ingestion/ingest_postings.py` calls JSearch, then uploads the response body to:

`/Volumes/workspace/bronze/raw_data/postings_<timestamp>.json`

### 2. Bronze → silver

`src/transformation/bronze_to_silver.py`:

- Reads all JSON in the bronze volume
- Explodes `data.jobs` and flattens fields (title, employer, location, salary, description, apply link, etc.)
- Drops duplicate / null `job_id`
- Adds:
  - `skills` — canonical names matched from a skills dictionary
  - `extracted_min_years_experience`
  - `extracted_education_level`
- Merges into `workspace.silver.job_postings` on `job_id` (insert or update)
- Sets a Databricks Jobs task value `silver_rows_changed` (`true` / `false`) so a downstream gold task can skip when nothing changed

### 3. Silver → gold

`src/transformation/silver_to_gold.py` overwrites three tables:

| Table | Grain | Metrics |
| --- | --- | --- |
| `skill_demand` | skill | posting count |
| `experience_requirements` | buckets `0-2`, `3-5`, `5-8`, `8+`, `Not Found` | count |
| `location_salary_trend` | city + state | posting count, average min/max **annual** salary |

Salary is normalized to yearly using period multipliers (year / month / week / day / hour). Rows missing city, state, or salary are excluded from the location table.

### 4. Dashboard

`src/dashboard/app.py` reads the gold tables via Databricks SQL and shows:

- KPI strip (distinct skills, top skill, cities hiring)
- Top-N skill demand bar chart
- Experience-range bar chart
- Filterable location / salary table

Gold queries are cached for 1 hour (`@st.cache_data(ttl=3600)`).

## Running locally

Install the Databricks Connect / environment deps, then run stages from a Databricks-connected session (cluster or serverless) so `DatabricksSession` and `WorkspaceClient` can reach the workspace.

```bash
# example: Streamlit after gold tables exist
streamlit run src/dashboard/app.py
```

Ingestion and Spark transforms are written as scripts meant to run as **Databricks Jobs tasks** (they create a session / workspace client at import time). Wire them as:

1. Ingest bronze JSON
2. Bronze → silver
3. Silver → gold (optionally only if `silver_rows_changed` is `true`)
4. (Optional) open or refresh the Streamlit app

## Skills extraction

`keyword_extractor.py` maps job-description regexes to canonical names across categories such as languages, Spark/Kafka, cloud platforms, warehouses, orchestration, and DevOps. Experience and education use similar regexes on the same text column.

## Notes

- `.env`, `.databricks`, `databricks.yml`, and `.streamlit` are gitignored — keep secrets out of the repo.
- Gold tables are full overwrites, not incremental merges.
- Ingest currently overwrites a single timestamped file per run; silver reads the **entire** bronze volume, so historical JSON files accumulate into the silver merge.
