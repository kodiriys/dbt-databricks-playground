WITH silver_medical_activity AS (
    SELECT * FROM {{ ref('fact_medical_activity') }}
)

SELECT * FROM silver_medical_activity
