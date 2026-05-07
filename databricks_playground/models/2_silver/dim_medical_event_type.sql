WITH medical_activity AS (
    SELECT * FROM {{ ref('medical_activity') }}
),

distinct_event_types AS (
    SELECT DISTINCT
        medical_event_type
    FROM medical_activity
    WHERE medical_event_type IS NOT NULL
),

enriched_event_types AS (
    SELECT
        medical_event_type,
        ROW_NUMBER() OVER (ORDER BY medical_event_type) AS medical_event_type_key,
        UPPER(medical_event_type) AS medical_event_type_normalized,
        CURRENT_TIMESTAMP() AS processed_at
    FROM distinct_event_types
)

SELECT * FROM enriched_event_types
ORDER BY medical_event_type_key
