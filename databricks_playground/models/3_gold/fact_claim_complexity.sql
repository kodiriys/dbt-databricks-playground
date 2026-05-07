WITH claims AS (
    SELECT * FROM {{ ref('dim_claims') }}
),

medical_activity AS (
    SELECT * FROM {{ ref('fact_medical_activity') }}
),

medical_event_types AS (
    SELECT * FROM {{ ref('dim_medical_event_type') }}
),

-- Get medical activity details with event type names
medical_activity_details AS (
    SELECT
        ma.claim_id,
        met.medical_event_type,
        ma.medical_event_date
    FROM medical_activity ma
    INNER JOIN medical_event_types met
        ON ma.medical_event_type_key = met.medical_event_type_key
),

-- Calculate complexity scores for each claim
claim_complexity AS (
    SELECT
        c.claim_id,
        -- Body part score: +15 if spine, head, or back
        CASE
            WHEN LOWER(c.body_part) IN ('spine', 'head', 'back') THEN 15
            ELSE 0
        END AS body_part_score,
        -- Litigation score: +20 if litigated
        CASE
            WHEN c.is_litigated = TRUE THEN 20
            ELSE 0
        END AS litigation_score,
        -- Prior claims score: +10 if more than 2 priors
        CASE
            WHEN c.number_of_priors > 2 THEN 10
            ELSE 0
        END AS prior_claims_score,
        -- Surgery score: +25 if at least one surgery
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM medical_activity_details mad
                WHERE mad.claim_id = c.claim_id
                AND LOWER(mad.medical_event_type) LIKE '%surgery%'
            ) THEN 25
            ELSE 0
        END AS surgery_score,
        -- Opioid score: +15 if at least one opioid prescription
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM medical_activity_details mad
                WHERE mad.claim_id = c.claim_id
                AND LOWER(mad.medical_event_type) LIKE '%opioid%'
            ) THEN 15
            ELSE 0
        END AS opioid_score
    FROM claims c
),

-- Calculate total complexity and determine complexity level
total_complexity AS (
    SELECT
        cc.claim_id,
        cc.body_part_score,
        cc.litigation_score,
        cc.prior_claims_score,
        cc.surgery_score,
        cc.opioid_score,
        (cc.body_part_score + cc.litigation_score + cc.prior_claims_score +
         cc.surgery_score + cc.opioid_score) AS total_complexity_score,
        CASE
            WHEN (cc.body_part_score + cc.litigation_score + cc.prior_claims_score +
                  cc.surgery_score + cc.opioid_score) >= 50 THEN 'High'
            WHEN (cc.body_part_score + cc.litigation_score + cc.prior_claims_score +
                  cc.surgery_score + cc.opioid_score) >= 25 THEN 'Medium'
            ELSE 'Low'
        END AS complexity_level,
        CURRENT_TIMESTAMP() AS processed_at
    FROM claim_complexity cc
)

SELECT * FROM total_complexity
ORDER BY total_complexity_score DESC
