-- Note: includes non-NYC counties, and rows for 
SELECT
    "County" AS county_name,
    "County" IN ('Kings', 'Queens', 'New York', 'Bronx', 'Richmond') AS is_nyc,
    "ZIP Code" as zip_code,
    NULLIF(REPLACE("Numerator", ',', ''), 's')::INT AS opioid_burden,
    NULLIF(REPLACE("Rate", '*', ''), 's')::FLOAT AS opioid_burden_rate_per_100k
from {{ ref('ny_zip_opioid_burden_2019_raw') }}
