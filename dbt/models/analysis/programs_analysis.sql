/*
Note: includes non-OTPs!
*/

with y as (
    select 
        program_number,
        current_certified_capacity
    from {{ ref('program_capacities_2019') }}
), sh as (
    select
        program_number,
        sum(total_admissions) as total_admissions,
        sum(coalesce(total_admissions, 1)) as total_admissions_1,
        sum(coalesce(total_admissions, 3)) as total_admissions_3,
        sum(coalesce(total_admissions, 5)) as total_admissions_5
    from {{ ref('program_admissions_2019') }}
        join {{ ref('programs') }} as p using (program_number)
    group by 1
), ca as (
    select
        program_number,
        sum(total_admissions) as total_admissions,
        sum(coalesce(total_admissions, 1)) as total_admissions_1,
        sum(coalesce(total_admissions, 3)) as total_admissions_3,
        sum(coalesce(total_admissions, 5)) as total_admissions_5
    from {{ ref('program_admissions_2017') }}
        join {{ ref('programs') }} as p using (program_number)
    group by 1
), ch as (
    select
        program_number,
        sum(case when year = '2010' then avg_daily_enrollment end) as avg_daily_enrollment_2010,
        sum(case when year = '2015' then avg_daily_enrollment end) as avg_daily_enrollment_2015,
        sum(case when year = '2019' then avg_daily_enrollment end) as avg_daily_enrollment_2019
    from {{ ref('enrollment_by_year') }}
    group by 1
)
select 
    program_number,
    y.current_certified_capacity,
    sh.total_admissions_1 AS total_admissions_1_2019,
    sh.total_admissions_3 AS total_admissions_3_2019,
    sh.total_admissions_5 AS total_admissions_5_2019,
    ca.total_admissions_1 AS total_admissions_1_2017,
    ca.total_admissions_3 AS total_admissions_3_2017,
    ca.total_admissions_5 AS total_admissions_5_2017,
    ch.avg_daily_enrollment_2010,
    ch.avg_daily_enrollment_2015,
    ch.avg_daily_enrollment_2019
from y
    full outer join sh using (program_number)
    full outer join ca using (program_number)
    full outer join ch using (program_number)
    full outer join {{ ref('programs') }} as p using (program_number)
order by 1