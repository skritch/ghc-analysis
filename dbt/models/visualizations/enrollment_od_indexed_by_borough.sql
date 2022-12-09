with enrollment as (
    select
        p.borough_name,
        c.year,
        SUM(c.avg_daily_enrollment) AS enrollment
    from {{ ref('enrollment_by_year') }} c
        join {{ ref('programs') }} as p using (program_number)
    where p.program_category = 'Opioid Treatment Program'
    group by 1, 2
),
e_and_od as (
    select 
        borough_name,
        year,
        enrollment,
        ny_county_od_trend.deaths
    from enrollment
        join {{ ref('ny_county_od_trend') }} using (borough_name, year)

    union 
    
    select 
        'NYC Total',
        year,
        sum(enrollment) as enrollment,
        sum(ny_county_od_trend.deaths) as deaths
    from enrollment
        join {{ ref('ny_county_od_trend') }} using (borough_name, year)
    group by year
)
select
    e2010.borough_name,
    -- e2010.enrollment as enrollment_2010,
    -- e2019.enrollment as enrollment_2019,
    -- e2019.enrollment - e2010.enrollment as enrollment_delta,
    -- e2010.deaths as deaths_2010,
    -- e2019.deaths as deaths_2019,
    -- e2019.deaths - e2010.deaths as deaths_delta,
    100 * (e2019.enrollment - e2010.enrollment)::float / e2010.enrollment as enrollment_pct_change,
    100 * (e2019.deaths - e2010.deaths)::float / e2010.deaths as deaths_pct_change
FROM e_and_od as e2010
join e_and_od as e2019
    on e2010.borough_name = e2019.borough_name
    and e2010.year = 2010 and e2019.year = 2019