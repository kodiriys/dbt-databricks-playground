WITH silver_medical_event_type AS (
    SELECT * FROM {{ ref('dim_medical_event_type') }}
)

SELECT * FROM silver_medical_event_type
