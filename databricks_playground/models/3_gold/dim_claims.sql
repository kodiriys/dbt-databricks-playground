WITH silver_claims AS (
    SELECT * FROM {{ ref('stg_dim_claims') }}
)

SELECT * FROM silver_claims
