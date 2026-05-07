WITH silver_date AS (
    SELECT * FROM {{ ref('stg_dim_date') }}
)

SELECT * FROM silver_date
