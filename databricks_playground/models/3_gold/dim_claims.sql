WITH silver_claims AS (
    SELECT * FROM {{ ref('dim_claims') }}
)

SELECT * FROM silver_claims
