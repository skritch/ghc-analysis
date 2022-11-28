select
    uhf_code,
    uhfs.borough_name,
    uhfs.is_harlem,
    sum(z.population_total) AS population_2020_estimate,
    sum(total_admissions_1_2017) as total_admissions_1_2017,
    sum(total_admissions_3_2017) as total_admissions_3_2017,
    sum(total_admissions_5_2017) as total_admissions_5_2017,
    sum(total_admissions_1_2019) as total_admissions_1_2019,
    sum(total_admissions_3_2019) as total_admissions_3_2019,
    sum(total_admissions_5_2019) as total_admissions_5_2019,
    sum(otp_capacity) AS otp_capacity,
    sum(avg_daily_enrollment_2010) AS avg_daily_enrollment_2010,
    sum(avg_daily_enrollment_2015) AS avg_daily_enrollment_2015,
    sum(avg_daily_enrollment_2019) AS avg_daily_enrollment_2019,
    sum(patient_admissions_3_2017) AS patient_admissions_3_2017,
    sum(patient_admissions_3_2019) AS patient_admissions_3_2019,
    sum(opioid_burden_2019) AS opioid_burden_2019,
    sum(od.overdose_deaths) AS overdose_deaths_2020
from {{ ref('zip_code_otp_analysis') }}
    left join {{ ref('zip_codes') }} AS z using (zip_code)
    left join {{ ref('uhfs') }} AS uhfs using (uhf_code)
    left join {{ ref('uhf_overdoses_2020') }} as od using (uhf_code)
where uhf_code is not null
group by 1, 2, 3