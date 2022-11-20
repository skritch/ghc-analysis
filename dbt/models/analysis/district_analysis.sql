select
    borough_district_code,
    d.is_harlem,
    program_category,
    d.population_2020 AS district_population,
    -- TODO: add admissions from within harlem only
    sum(total_admissions_1_2017) as total_admissions_1_2017,
    sum(total_admissions_3_2017) as total_admissions_3_2017,
    sum(total_admissions_5_2017) as total_admissions_5_2017,
    sum(total_admissions_1_2019) as total_admissions_1_2019,
    sum(total_admissions_3_2019) as total_admissions_3_2019,
    sum(total_admissions_5_2019) as total_admissions_5_2019,
    sum(current_certified_capacity) AS current_certified_capacity,
    sum(avg_daily_enrollment_2010) AS avg_daily_enrollment_2010,
    sum(avg_daily_enrollment_2015) AS avg_daily_enrollment_2015,
    sum(avg_daily_enrollment_2019) AS avg_daily_enrollment_2019
from {{ ref('programs_analysis') }}
    left join {{ ref('programs') }} as p using (program_number)
    left join {{ ref('districts') }} as d using (borough_district_code)
-- A couple of programs can't be match to a district
where borough_district_code is not null
group by 1, 2, 3, 4
