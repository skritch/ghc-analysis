with enrollment as (
    select
        p.borough_name,
        c.year,
        SUM(c.avg_daily_enrollment) AS enrollment
    from {{ ref('enrollment_by_year') }} c
        join {{ ref('programs') }} as p using (program_number)
    where p.program_category = 'Opioid Treatment Program'
    group by 1, 2
)
select
    year,
    sum(case when borough_name = 'Bronx' then enrollment end) as "Bronx",
    sum(case when borough_name = 'Brooklyn' then enrollment end) as "Brooklyn",
    sum(case when borough_name = 'Manhattan' then enrollment end) as "Manhattan",
    sum(case when borough_name = 'Queens' then enrollment end) as "Queens",
    sum(case when borough_name = 'Staten Island' then enrollment end) as "Staten Island"
from enrollment AS e
where year in (2010, 2015, 2019)
group by 1
order by 1