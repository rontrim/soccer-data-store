# GitHub Copilot Instructions for Soccer Data Store

This project is a **Databricks Asset Bundle (DAB)** implementing a **Medallion Architecture** (Bronze/Silver/Gold) for soccer analytics, with a **Streamlit** frontend.

## 🏗 Project Architecture

- **Infrastructure as Code (`resources/`)**:
  - `pipeline.yml`: Defines Delta Live Tables (DLT) pipelines for Bronze, Silver, and Gold layers.
  - `club_soccer_data_pipeline.yml`: Defines the Databricks Job workflow (Check -> Ingest -> Process).
- **Data Transformation (`club-soccer-data/`)**:
  - Contains the actual logic for DLT pipelines (SQL and Python).
  - `bronze-layer/`: Raw data ingestion and initial checks.
  - `silver-layer/`: Cleaning and standardization (`results.py`).
  - `gold-layer/`: Aggregations for analysis (`headline_form_table.py`).
- **Ingestion (`ingestion/`)**:
  - Standalone Python scripts for fetching raw data.
  - `run_ingestion_scripts.py`: Orchestrator for local/manual ingestion.
- **Frontend (`soccer data app/`)**:
  - `app.py`: Streamlit dashboard connecting to Databricks SQL Warehouse.

## 🚀 Developer Workflows

- **Databricks Asset Bundle (DAB)**:
  - Use `databricks bundle validate` to check YAML configuration.
  - Use `databricks bundle deploy` to deploy resources to the workspace.
  - Use `databricks bundle run <job_key>` to trigger jobs manually.
- **Streamlit App**:
  - Run locally: `streamlit run "soccer data app/app.py"`.
  - Requires `secrets.toml` (or `st.secrets`) with `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, and `SQL_HTTP_PATH`.

## 🧩 Key Patterns & Conventions

### Databricks Resources (YAML)
- **Dynamic References**: Use `${resources.pipelines.<pipeline_name>.id}` to reference pipeline IDs in job tasks.
- **Task Keys**: Use descriptive `task_key` (e.g., `New_matches_check`, `ingest_club_soccer_data`) to define dependencies.
- **Relative Paths**: In `databricks.yml` and resource YAMLs, file paths are relative to the bundle root.

### Data Engineering (Python/SQL)
- **Path Handling**: Use `os.path.dirname(os.path.abspath(__file__))` in Python scripts to ensure robust file access regardless of execution context.
- **Spark/DLT**: Transformation scripts in `club-soccer-data/` are designed to run as DLT notebooks or Spark jobs.
- **Medallion Flow**:
  1.  **Bronze**: Ingest raw JSON/CSV to Delta.
  2.  **Silver**: Clean, deduplicate, and schema enforcement.
  3.  **Gold**: Business-level aggregates (e.g., League Tables, Form Tables).

### Frontend (Streamlit)
- **Data Access**: Uses `databricks.sql.connect` to query Gold tables (`soccer_data.analyze`).
- **Caching**: Use `@st.cache_data(ttl=3600)` for expensive SQL queries.
- **UI Components**: Use `st.column_config` for formatting dataframes (e.g., Progress bars for possession).

## ⚠️ Critical Integration Points
- **Job Orchestration**: The job `Club_Soccer_Data_Processing` relies on the output of `recent_matches_check.py` (row count) to decide whether to trigger the DLT pipeline.
- **Secrets Management**: The app relies on Streamlit secrets; ensure these are configured in the environment or `.streamlit/secrets.toml`.
