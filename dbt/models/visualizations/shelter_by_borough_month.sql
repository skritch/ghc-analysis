SELECT
    report_date::date,
    sum(case when borough_name = 'Bronx' then census_total end) as "Bronx",
    sum(case when borough_name = 'Brooklyn' then census_total end) as "Brooklyn",
    sum(case when borough_name = 'Manhattan' then census_total end) as "Manhattan",
    sum(case when borough_name = 'Queens' then census_total end) as "Queens",
    sum(case when borough_name = 'Staten Island' then census_total end) as "Staten Island"
FROM {{ ref('shelter_population') }}
WHERE census_total IS NOT NULL
GROUP BY 1
ORDER BY 1