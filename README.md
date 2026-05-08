# Databricks Playground with DBT Medallion Architecture

# Overview

## Pipeline

Note: I have set up two environments:

1. `medallion_stage` - Staging environment
2. `medallion_prod` - Production environment

I have configured DBT to build to either of these target environments, with `stage` being the default.

Pipeline:

- Ingest data from SFTP
    - `Fivetran` Scheduled to run daily (24 hours) -> Merge Mode -> Databricks table in `workspace.raw_sftp.claims`
        - I assumed daily CSVs would be in this format: `YYYYMMDD_claims_data.csv`
        - I set up Fivetran to use this file pattern to detect files in the SFTP directory and merge them: `.*_claims_data.csv`
    - `Python Notebook` scheduled to run daily saves data into `medallion_{stage|prod}.0_landing.claims`
- Ingest data from API
    - `Python Notebook` scheduled to run once an hour reaches out to simulated API to fetch medical activity data
    - Saves data into `medallion_{stage|prod}.0_landing.medical_activity`

Transform data from `0_landing` into a medallion architecture using DBT. (See DBT lineage graph using the DBT extension.)

Data is transformed into `1_bronze`, `2_silver`, and `3_gold` schemas using DBT models.

DBT contains tests to ensure data quality and integrity.

Tools used:

- `DBT` (Data Build Tool) - for transforming data into the medallion architecture
- `Databricks` - for storing and managing data, PySpark notebooks, and scheduling basic ETL jobs
- `Fivetran` - for ingesting data from SFTP
- `Python` - for data manipulation and API calls
- `SQL` - for querying data in Databricks and modelling data using DBT
- `PySpark` - for processing data in Databricks Notebooks
- `Git` - for version control
- `UV` - for Python dependency management
- `Airflow` - This was not used in this simple project but for a production environment I would set it up to orchestrate DBT jobs and schedule them using the Kubernetes orchestrator

# Input sample data

- From SFTP: `claims_data.csv`
    - Headers: `Claim_id,Date_of_loss,Body_part,Is_litigated,Number_of_priors`
- From API: `medical_activity.json`
    - Format:
    ```json
    {
        "claim_id": "C123",
        "medical_events": [
            { "type": "surgery", "date": "2025-06-01" },
            { "type": "opioid prescription", "date": "2025-06-02" }
        ]
    }
    ```

## Data ingestion into Databricks

See notebooks in `Notebooks` folder.
I have set up jobs in Databricks that run these notebooks on a schedule.

# Databricks

Created catalog called `medallion`.
Created schemas in catalog `medallion`:

- `0_landing`
- `1_bronze`
- `2_silver`
- `3_gold`

For medallion architecture.

## DBT

```sh
uv init
uv add dbt-core dbt-databricks
uv sync
source .venv/bin/activate
dbt init
cd databricks_playground
dbt debug
# 02:09:31  All checks passed!

# To run DBT models individually
dbt run -s "1_bronze"
dbt run -s "2_silver"
dbt run -s "3_gold"

# Or run all models
dbt run

# Then perform data quality checks
dbt test
```

Building for stage vs production, if you check `profiles.example.yml` you can see the differences between the two targets.

```sh
# Default is set to stage
dbt run --target stage
dbt run --target prod
dbt test --target stage
dbt test --target prod
dbt build --target stage
dbt build --target prod
```
