WITH medical_activity AS (
    SELECT * FROM {{ ref('medical_activity') }}
),

medical_event_types AS (
    SELECT * FROM {{ ref('dim_medical_event_type') }}
),

enriched_medical_activity AS (
    SELECT
        ma.claim_id,
        met.medical_event_type_key,
        ma.medical_event_type,
        ma.medical_event_date,
        CAST(ma.medical_event_date AS DATE) AS medical_event_date_key,
        CURRENT_TIMESTAMP() AS processed_at
    FROM medical_activity ma
    INNER JOIN medical_event_types met
        ON ma.medical_event_type = met.medical_event_type
    WHERE ma.claim_id IS NOT NULL
)

SELECT * FROM enriched_medical_activity
