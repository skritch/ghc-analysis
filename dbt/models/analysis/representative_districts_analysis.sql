

with zip_code_metrics as (
    select
        z.zip_code,
        z.borough_name,
        z.is_harlem,
        patient_admissions_3_2019,
        harlem_patient_admissions_3_2019,
        -- FROM harlem?
        opioid_burden_2019,
        -- QAing columns
        population_2020_estimate,
        total_admissions_3_2019,
        otp_capacity,
        avg_daily_enrollment_2019
    from {{ref('zip_codes')}} z
        left join {{ref('zip_code_otp_analysis')}} za on z.zip_code = za.zip_code 
)
, all_districts as (
    select
        district_type,
        district,
        district_type || ' ' || district::text as district_name,
        representative_name,
        representative_party,
        boundary,
        null as borough_name,
        population_2020 
    from {{ref('representative_districts')}}

    union all

    select
        'Community District' as district_type,
        borough_district_code as district,
        district_name,
        null as representative_name,
        null as representative_party,
        boundary,
        borough_name,
        population_2020
    from {{ref('community_districts')}}
),
district_metrics_from_zips as (
    SELECT
        district_type,
        district,

        sum(patient_admissions_3_2019 * f_of_zip_area) as patient_admissions_3_2019,
        sum(harlem_patient_admissions_3_2019 * f_of_zip_area) as harlem_patient_admissions_3_2019,
        sum(opioid_burden_2019 * f_of_zip_area) as opioid_burden_2019,
        -- QAing columns
        sum(m.population_2020_estimate * f_of_zip_area) as population_2020_estimate,
        sum(total_admissions_3_2019 * f_of_zip_area) as total_admissions_3_2019,
        sum(otp_capacity * f_of_zip_area) as otp_capacity,
        sum(avg_daily_enrollment_2019 * f_of_zip_area) as avg_daily_enrollment_2019
    FROM all_districts
        LEFT JOIN {{ref('zip_to_district')}} using (district_type, district)
        LEFT JOIN zip_code_metrics as m USING (zip_code)
    group by 1, 2
)
, district_metrics_from_uhfs as (
    SELECT
        district_type,
        district,
        sum(overdose_deaths_2020 * f_of_uhf_population) AS overdose_deaths_2020,
        -- QAing columns
        sum(u.population_2020_estimate * f_of_uhf_population) as population_2020_estimate

    FROM all_districts
        LEFT JOIN {{ref('uhf_to_district')}} using (district_type, district)
        LEFT JOIN {{ref('uhf_otp_analysis')}} as u using (uhf_code)
    group by 1, 2
),
district_program_metrics as (
    select
        district_type,
        district,
        count(*) as ct_otp_programs,
        sum(current_certified_capacity) as otp_capacity,
        sum(total_admissions_3_2019) as total_admissions_3_2019,
        sum(avg_daily_enrollment_2019) as avg_daily_enrollment_2019
    from all_districts as d
        left join {{ref('programs')}} as p on ST_Intersects(d.boundary, p.location)
        join {{ref('programs_analysis')}} using (program_number)
    where program_category = 'Opioid Treatment Program'
    group by 1, 2
)
select
    district_type,
    district,
    district_name,
    representative_name,
    representative_party,
    d.population_2020,
    coalesce(z.patient_admissions_3_2019, 0) as patient_admissions_3_2019,
    coalesce(z.harlem_patient_admissions_3_2019, 0) as harlem_patient_admissions_3_2019,
    coalesce(z.opioid_burden_2019, 0) as opioid_burden_2019,
    coalesce(u.overdose_deaths_2020, 0) as overdose_deaths_2020,
    coalesce(p.ct_otp_programs, 0) as ct_otp_programs,
    coalesce(p.total_admissions_3_2019, 0) as total_admissions_3_2019,
    -- coalesce(z.total_admissions_3_2019, 0) as total_admissions_3_2019_from_zip,
    coalesce(p.otp_capacity, 0) as otp_capacity,
    -- coalesce(z.otp_capacity, 0) as otp_capacity_from_zip,
    coalesce(p.avg_daily_enrollment_2019, 0) as avg_daily_enrollment_2019
    -- coalesce(z.avg_daily_enrollment_2019, 0) as avg_daily_enrollment_2019_from_zip,

from all_districts as d
    left join district_metrics_from_zips as z using (district_type, district)
    left join district_metrics_from_uhfs as u using (district_type, district)
    left join district_program_metrics as p using (district_type, district)
order by 1, 2

