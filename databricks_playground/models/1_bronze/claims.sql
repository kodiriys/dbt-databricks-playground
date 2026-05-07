SELECT
    *
FROM {{ source('landing', 'claims') }}
