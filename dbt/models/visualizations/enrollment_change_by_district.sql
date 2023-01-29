with enrollment as (
    select
        p.borough_district_code,
        c.year,
        SUM(c.avg_daily_enrollment) AS enrollment
    from {{ ref('enrollment_by_year') }} c
        join {{ ref('programs') }} as p using (program_number)
        join {{ ref('community_districts') }} as d on p.borough_district_code = d.borough_district_code
    where p.program_category = 'Opioid Treatment Program'
    group by 1, 2
)
select
    d.borough_district_code "Boro CD code",
    d.district_name as "District Name",
    coalesce(e2010.enrollment, 0) as "2010 Daily Avg. Enrollment",
    coalesce(e2019.enrollment, 0) as "2019 Daily Avg. Enrollment",
    coalesce(e2019.enrollment, 0) - coalesce(e2010.enrollment, 0) as "Change in Daily Avg. Enrollment",
    100 * (coalesce(e2019.enrollment, 0) - e2010.enrollment)::float / e2010.enrollment as "Pct Change in Daily Avg Enrollment"
FROM {{ ref('community_districts') }} as d
    left join enrollment as e2010 
        on d.borough_district_code = e2010.borough_district_code
        and e2010.year = 2010
    left join enrollment as e2019
        on d.borough_district_code = e2019.borough_district_code
        and e2019.year = 2019
order by 1