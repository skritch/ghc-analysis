with e as (
    select
        year,
        SUM(c.avg_daily_enrollment) AS enrollment,
        SUM(case when p.program_category = 'Opioid Treatment Program'
            then c.avg_daily_enrollment end) as otp_enrollment
    from {{ ref('enrollment_by_year') }} c
        left join {{ ref('programs') }} p using (program_number)
    group by 1
),
ods as (
    select
        year,
        sum(deaths) as deaths
    from {{ ref('ny_county_od_trend') }}
    where is_nyc
    group by 1
),
t as (
    select
        ods.year,
        e.enrollment,
        deaths,
        e_next.enrollment as next_e,
        e_prev.enrollment as prev_e,
        e_next.year as next_yr,
        e_prev.year as prev_yr
    from ods
        left join e using (year)
        left join lateral (
            select e_inner.year, enrollment 
            from e as e_inner
            where e_inner.year <= ods.year
            order by e_inner.year desc 
            limit 1
        ) e_prev ON TRUE
        left join lateral (
            select e_inner.year, enrollment 
            from e as e_inner
            where e_inner.year >= ods.year
            order by e_inner.year asc 
            limit 1
        ) e_next ON TRUE
),
interpolated as (
    select
        year,
        deaths,
        enrollment is null as is_interpolated,
        coalesce(
            enrollment,
            prev_e + (next_e - prev_e)::float * (year - prev_yr) / (next_yr - prev_yr)
        ) as enrollment_interpolated
    from t
)
select
    i.year,
    i.is_interpolated,
    case when i.enrollment_interpolated is not null then 'yes' else 'no' end,
    100 * i.enrollment_interpolated :: float / t2010.enrollment as enrollment_pct_change,
    i.deaths,
    100 * i.deaths :: float / t2010.deaths as deaths_pct_change
from interpolated as i
    join t as t2010 on t2010.year = 2010
order by i.year