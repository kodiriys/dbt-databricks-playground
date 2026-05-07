SELECT
    *
FROM {{ source('landing', 'medical_activity') }}
