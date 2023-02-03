with patients_2019 as (
    select
        patient_zip_code AS zip_code,
        sum(coalesce(total_admissions, 1)) as total_admissions_1,
        sum(coalesce(total_admissions, 3)) as total_admissions_3,
        sum(coalesce(total_admissions, 5)) as total_admissions_5,
        sum(case when p.address_zip_code = patient_zip_code
            then coalesce(total_admissions, 3) end) as in_zip_admissions_3,
        sum(case when p.borough_district_code in (109, 110, 111) 
            then coalesce(total_admissions, 3) end) as harlem_admissions_3
    from {{ ref('program_admissions_2019') }}
        join {{ ref('programs') }} as p using (program_number)
    where p.program_category = 'Opioid Treatment Program'
    group by 1
), patients_2017 as (
    select
        patient_zip_code AS zip_code,
        sum(coalesce(total_admissions, 1)) as total_admissions_1,
        sum(coalesce(total_admissions, 3)) as total_admissions_3,
        sum(coalesce(total_admissions, 5)) as total_admissions_5,
        sum(case when p.address_zip_code = patient_zip_code
            then coalesce(total_admissions, 3) end) as in_zip_admissions_3,
        sum(case when p.borough_district_code in (109, 110, 111) 
            then coalesce(total_admissions, 3) end) as harlem_admissions_3
    from {{ ref('program_admissions_2017') }}
        join {{ ref('programs') }} as p using (program_number)
    where p.program_category = 'Opioid Treatment Program'
    group by 1
),
programs_by_zip AS (
    select
        p.address_zip_code AS zip_code,
        sum(total_admissions_1_2017) as total_admissions_1_2017,
        sum(total_admissions_3_2017) as total_admissions_3_2017,
        sum(total_admissions_5_2017) as total_admissions_5_2017,
        sum(total_admissions_1_2019) as total_admissions_1_2019,
        sum(total_admissions_3_2019) as total_admissions_3_2019,
        sum(total_admissions_5_2019) as total_admissions_5_2019,
        sum(current_certified_capacity) AS otp_capacity,
        sum(avg_daily_enrollment_2010) AS avg_daily_enrollment_2010,
        sum(avg_daily_enrollment_2015) AS avg_daily_enrollment_2015,
        sum(avg_daily_enrollment_2019) AS avg_daily_enrollment_2019
    from {{ ref('programs_analysis') }} pa
        join {{ ref('programs') }} as p using (program_number)
    where p.program_category = 'Opioid Treatment Program'
    group by 1
)
select
    zip_code,
    zip_codes.is_harlem,
    zip_codes.near_harlem,
    zip_codes.borough_name,
    zip_codes.neighborhood_name,
    zip_codes.population_2020 as population_2020_estimate,
    programs_by_zip.total_admissions_1_2017,
    programs_by_zip.total_admissions_3_2017,
    programs_by_zip.total_admissions_5_2017,
    programs_by_zip.total_admissions_1_2019,
    programs_by_zip.total_admissions_3_2019,
    programs_by_zip.total_admissions_5_2019,
    programs_by_zip.otp_capacity,
    programs_by_zip.avg_daily_enrollment_2010,
    programs_by_zip.avg_daily_enrollment_2015,
    programs_by_zip.avg_daily_enrollment_2019,
    patients_2017.total_admissions_3 AS patient_admissions_1_2017,
    patients_2017.total_admissions_3 AS patient_admissions_3_2017,
    patients_2017.total_admissions_3 AS patient_admissions_5_2017,
    patients_2017.in_zip_admissions_3 AS in_zip_admissions_3_2017,
    patients_2017.harlem_admissions_3 AS harlem_patient_admissions_3_2017,
    patients_2019.total_admissions_3 AS patient_admissions_1_2019,
    patients_2019.total_admissions_3 AS patient_admissions_3_2019,
    patients_2019.total_admissions_3 AS patient_admissions_5_2019,
    patients_2019.in_zip_admissions_3 AS in_zip_admissions_3_2019,
    patients_2019.harlem_admissions_3 AS harlem_patient_admissions_3_2019,
    op.opioid_burden AS opioid_burden_2019

    -- add travel time
from {{ ref('zip_codes') }}  
    left outer join programs_by_zip using (zip_code) 
    left outer join patients_2019 using (zip_code)
    left outer join patients_2017 using (zip_code)
    left outer join {{ ref('ny_zip_opioid_burden_2019') }} op using (zip_code)