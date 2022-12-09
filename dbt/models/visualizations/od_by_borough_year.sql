with ods as (
    select
        borough_name,
        year,
        deaths
    from {{ ref('ny_county_od_trend') }}
    where is_nyc
)
select
    year,
    sum(case when borough_name = 'Bronx' then deaths end) as "Bronx",
    sum(case when borough_name = 'Brooklyn' then deaths end) as "Brooklyn",
    sum(case when borough_name = 'Manhattan' then deaths end) as "Manhattan",
    sum(case when borough_name = 'Queens' then deaths end) as "Queens",
    sum(case when borough_name = 'Staten Island' then deaths end) as "Staten Island"
from ods AS e
group by 1
order by 1