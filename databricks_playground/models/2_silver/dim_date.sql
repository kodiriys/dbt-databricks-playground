WITH date_range AS (
    SELECT
        MIN(CAST(medical_event_date AS DATE)) AS min_date,
        MAX(CAST(medical_event_date AS DATE)) AS max_date
    FROM {{ ref('medical_activity') }}
),

date_series AS (
    SELECT explode(sequence(
        (SELECT min_date FROM date_range),
        (SELECT max_date FROM date_range),
        INTERVAL 1 DAY
    )) AS date_value
),

enriched_dates AS (
    SELECT
        date_value AS date_key,
        YEAR(date_value) AS year,
        MONTH(date_value) AS month,
        DAY(date_value) AS day,
        QUARTER(date_value) AS quarter,
        WEEKOFYEAR(date_value) AS week_of_year,
        DAYOFWEEK(date_value) AS day_of_week,
        DAYOFYEAR(date_value) AS day_of_year,
        DATE_FORMAT(date_value, 'EEEE') AS day_name,
        DATE_FORMAT(date_value, 'MMMM') AS month_name,
        DATE_FORMAT(date_value, 'yyyy-MM-dd') AS date_id,
        DATE_FORMAT(date_value, 'yyyy-MM') AS year_month,
        DATE_FORMAT(date_value, 'yyyy-Q') AS year_quarter,
        CASE
            WHEN DAYOFWEEK(date_value) IN (1, 7) THEN TRUE
            ELSE FALSE
        END AS is_weekend,
        CASE
            WHEN DAYOFWEEK(date_value) IN (2, 3, 4, 5, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekday,
        LAST_DAY(date_value) AS month_end_date,
        DATE_ADD(LAST_DAY(date_value), 1) AS next_month_start_date
    FROM date_series
)

SELECT * FROM enriched_dates
ORDER BY date_key
