with t as (
    select
        date_trunc('month', arrest_date) as month,
        ct.borough_district_code,
        sum((offense_category = 'Property')::int) as "Property Arrests",
        sum((offense_category = 'Disorder')::int) as "Disorder Arrests",
        sum((offense_category = 'Drugs')::int) as "Drug Arrests",
        sum((offense_category = 'Major')::int) as "Major Arrests",
        sum((offense_level = 'F')::int) as "Felony Arrests"
    from {{ ref('arrests') }}
        join {{ ref('census_tracts') }} as ct using (boroct2020)
    group by 1, 2
)
select 
    month,
    borough_district_code,
    district_name,
    population_2020 AS district_population, 
    "Property Arrests",
    1000 * "Property Arrests"::float / population_2020 AS "Property Arrests per 1k",
    "Disorder Arrests",
    1000 * "Disorder Arrests"::float / population_2020 AS "Disorder Arrests per 1k",
    "Drug Arrests",
    1000 * "Drug Arrests"::float / population_2020 AS "Drug Arrests per 1k",
    "Major Arrests",
    1000 * "Major Arrests"::float / population_2020 AS "Major Arrests per 1k",
    "Felony Arrests",
    1000 * "Felony Arrests"::float / population_2020 AS "Felony Arrests per 1k"
from t
    join {{ ref('districts') }} as d using (borough_district_code)
