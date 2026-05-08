SELECT
    claim_id,
    complexity_level,
    total_complexity_score,
    date_format(processed_at, 'yyyy-MM-dd') AS processed_at
FROM
    {{ ref('fact_claim_complexity') }}
WHERE
    upper(complexity_level) = upper('High')
    AND
    processed_at >= getdate() - INTERVAL 7 DAYS
ORDER BY
    total_complexity_score DESC,
    processed_at DESC
