WITH claims AS (
    SELECT * FROM {{ ref('claims') }}
),

enriched_claims AS (
    SELECT
        claim_id,
        date_of_loss,
        body_part,
        is_litigated,
        number_of_priors,
        CURRENT_TIMESTAMP() AS processed_at
    FROM claims
    WHERE claim_id IS NOT NULL
)

SELECT * FROM enriched_claims
