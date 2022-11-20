SELECT
    "UHF of residence" AS uhf_code,
    -- Use UHF table's name instead?
    "UHF Name" AS uhf_name,
    NULLIF("Number", 'X') AS overdose_deaths,
    NULLIF("Rate/100,000", 'X') :: FLOAT AS overdose_rate_per_100k
FROM {{ ref('od_by_uhf_2020_raw') }}
