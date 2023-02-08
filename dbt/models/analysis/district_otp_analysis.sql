select
    d.borough_district_code,
    d.is_harlem,
    d.population_2020,
    -- TODO: add admissions from within harlem only
    coalesce(sum(total_admissions_1_2017), 0) as total_admissions_1_2017,
    coalesce(sum(total_admissions_3_2017), 0) as total_admissions_3_2017,
    coalesce(sum(total_admissions_5_2017), 0) as total_admissions_5_2017,
    coalesce(sum(total_admissions_1_2019), 0) as total_admissions_1_2019,
    coalesce(sum(total_admissions_3_2019), 0) as total_admissions_3_2019,
    coalesce(sum(total_admissions_5_2019), 0) as total_admissions_5_2019,
    coalesce(sum(current_certified_capacity), 0) AS otp_capacity,
    coalesce(sum(avg_daily_enrollment_2010), 0) AS avg_daily_enrollment_2010,
    coalesce(sum(avg_daily_enrollment_2015), 0) AS avg_daily_enrollment_2015,
    coalesce(sum(avg_daily_enrollment_2019), 0) AS avg_daily_enrollment_2019
from community_districts as d
    left join {{ ref('programs') }} as p on
        d.borough_district_code = p.borough_district_code
        and p.program_category = 'Opioid Treatment Program'
    left join {{ ref('programs_analysis') }} as pa using (program_number)
-- A couple of programs can't be match to a district
group by 1, 2, 3