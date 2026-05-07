WITH silver_date AS (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT * FROM silver_date
