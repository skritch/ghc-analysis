SELECT
    "County Name" as county_name,
    "County Name" IN ('Kings', 'Queens', 'New York', 'Bronx', 'Richmond') AS is_nyc,
    CASE "County Name"
        WHEN 'Kings' THEN 'Brooklyn'
        WHEN 'Queens' THEN 'Queens'
        WHEN 'New York' THEN 'Manhattan'
        WHEN 'Bronx' THEN 'Bronx'
        WHEN 'Richmond' THEN 'Staten Island'
    END AS borough_name,
    "Event Count/Rate" AS deaths,
    "Average Number of Denominator/Rate" as total_population,
    "Percentage/Rate/Ratio" AS rate_per_100k,
    "Data Years" AS year
from {{ ref('ny_county_od_trend_raw') }}
