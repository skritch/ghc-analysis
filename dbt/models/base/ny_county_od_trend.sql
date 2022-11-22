SELECT
    "County Name" as county_name,
    "County Name" IN ('Kings', 'Queens', 'New York', 'Bronx', 'Richmond') AS is_nyc,
    {{ nyc_county_to_borough("County Name") }} AS borough_name,
    "Event Count/Rate" AS deaths,
    "Average Number of Denominator/Rate" as total_population,
    "Percentage/Rate/Ratio" AS rate_per_100k,
    "Data Years" AS year
from {{ ref('ny_county_od_trend_raw') }}
