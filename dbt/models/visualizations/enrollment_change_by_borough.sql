with ps as (
select
    p.borough_name,
    SUM(case when year = 2010 then c.avg_daily_enrollment end) AS enrollment_2010,
    SUM(case when year = 2019 then c.avg_daily_enrollment end) AS enrollment_2019,
    SUM(case when year = 2010 and p.program_category = 'Opioid Treatment Program' then c.avg_daily_enrollment end) as otp_enrollment_2010,
    SUM(case when year = 2019 and p.program_category = 'Opioid Treatment Program' then c.avg_daily_enrollment end) as otp_enrollment_2019
from {{ ref('enrollment_by_year') }} c
    join {{ ref('programs') }} as p using (program_number)
group by 1


union

select
    'NYC Total',
    SUM(case when year = 2010 then c.avg_daily_enrollment end) AS enrollment_2010,
    SUM(case when year = 2019 then c.avg_daily_enrollment end) AS enrollment_2019,
    SUM(case when year = 2010 and p.program_category = 'Opioid Treatment Program' then c.avg_daily_enrollment end) as otp_enrollment_2010,
    SUM(case when year = 2019 and p.program_category = 'Opioid Treatment Program' then c.avg_daily_enrollment end) as otp_enrollment_2019
from {{ ref('enrollment_by_year') }} c
    join {{ ref('programs') }} as p using (program_number)
)
select
    borough_name,
    coalesce(enrollment_2010, 0) as "2010 Daily Avg. Enrollment",
    coalesce(enrollment_2019, 0) as "2019 Daily Avg. Enrollment",
    coalesce(enrollment_2019, 0) - coalesce(enrollment_2010, 0) as "Change in Daily Avg. Enrollment",
    100 * (coalesce(enrollment_2019, 0) - enrollment_2010)::float / enrollment_2010 as "Pct Change in Daily Avg Enrollment",
    coalesce(otp_enrollment_2010, 0) as "2010 Daily Avg. Enrollment (OTP Only)",
    coalesce(otp_enrollment_2019, 0) as "2019 Daily Avg. Enrollment (OTP Only)",
    coalesce(otp_enrollment_2019, 0) - coalesce(otp_enrollment_2010, 0) as "Change in Daily Avg. Enrollment (OTP Only)",
    100 * (coalesce(otp_enrollment_2019, 0) - otp_enrollment_2010)::float / otp_enrollment_2010 as "Pct Change in Daily Avg Enrollment (OTP Only)"
from ps as p2010
order by (
    case when borough_name = 'NYC Total' then 'zzzz' else borough_name end
), 2