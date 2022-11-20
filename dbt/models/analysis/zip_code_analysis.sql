with shawn_patients as (
    select
        patient_zip_code AS zip_code,
        program_category,
        sum(coalesce(total_admissions, 1)) as total_admissions_1,
        sum(coalesce(total_admissions, 3)) as total_admissions_3,
        sum(coalesce(total_admissions, 5)) as total_admissions_5,
        sum(case when p.borough_district_code in (109, 110, 111) 
            then coalesce(total_admissions, 3) end) as harlem_admissions_3
    from {{ ref('shawn_foil') }}
        left join {{ ref('programs') }} as p using (program_number)
    group by 1, 2
), candace_patients as (
    select
        patient_zip_code AS zip_code,
        p.program_category,
        sum(coalesce(total_admissions, 1)) as total_admissions_1,
        sum(coalesce(total_admissions, 3)) as total_admissions_3,
        sum(coalesce(total_admissions, 5)) as total_admissions_5,
        sum(case when p.borough_district_code in (109, 110, 111) 
            then coalesce(total_admissions, 3) end) as harlem_admissions_3
    from {{ ref('candace_foil') }}
        left join {{ ref('programs') }} as p using (program_number)
    group by 1, 2
),
programs_by_zip AS (
    select
        p.address_zip_code AS zip_code,
        program_category,
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
    group by 1, 2
)
select
    zip_code,
    zip_codes.is_harlem,
    zip_codes.near_harlem,
    zip_codes.borough_name,
    zip_codes.neighborhood_name,
    zip_codes.zip_code IS NULL AS is_outside_nyc,
    zip_codes.population_total as population_2020_estimate,
    programs_by_zip.program_category,
    programs_by_zip.total_admissions_1_2017,
    programs_by_zip.total_admissions_3_2017,
    programs_by_zip.total_admissions_5_2017,
    programs_by_zip.total_admissions_1_2019,
    programs_by_zip.total_admissions_3_2019,
    programs_by_zip.total_admissions_5_2019,
    programs_by_zip.current_certified_capacity,
    programs_by_zip.avg_daily_enrollment_2010,
    programs_by_zip.avg_daily_enrollment_2015,
    programs_by_zip.avg_daily_enrollment_2019,
    candace_patients.total_admissions_3 AS patient_admissions_1_2017,
    candace_patients.total_admissions_3 AS patient_admissions_3_2017,
    candace_patients.total_admissions_3 AS patient_admissions_5_2017,
    candace_patients.harlem_admissions_3 AS harlem_patient_admissions_3_2017,
    shawn_patients.total_admissions_3 AS patient_admissions_1_2019,
    shawn_patients.total_admissions_3 AS patient_admissions_3_2019,
    shawn_patients.total_admissions_3 AS patient_admissions_5_2019,
    shawn_patients.harlem_admissions_3 AS harlem_patient_admissions_3_2019,
    op.opioid_burden AS opioid_burden_2019
from programs_by_zip
    full outer join shawn_patients using (zip_code, program_category)
    full outer join candace_patients using (zip_code, program_category)
    full outer join {{ ref('zip_codes') }} using (zip_code)
    left join {{ ref('ny_zip_opioid_burden_2019') }} op using (zip_code)