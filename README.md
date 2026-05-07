# dbt-databricks-playground

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
