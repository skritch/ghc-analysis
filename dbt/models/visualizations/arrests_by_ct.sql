select
    boroct2020,
    SUBSTRING(census_tracts.census_tract_geoid from 3) as tract_code,
    count(*) as ct,
    sum((offense_category = 'Major')::int) as ct_major
from arrests
    join census_tracts using (boroct2020)
where offense_category in ('Property', 'Disorder', 'Drugs', 'Major')
    and offense_level = 'F'
    and arrest_date >= '2021-01-01'
group by 1, 2