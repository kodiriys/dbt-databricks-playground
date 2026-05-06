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

# Databricks

Created schemas in workspace:

- raw
- silver
- gold
  For medallion architecture
