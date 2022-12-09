with by_zip as (
    SELECT
        is_harlem,
        borough_name,
        100 * sum(population_2020_estimate) :: float / sum(sum(population_2020_estimate)) over () as population_2020_estimate,
        100 * sum(total_admissions_3_2019) :: float / sum(sum(total_admissions_3_2019)) over () AS total_admissions_3_2019,
        100 * sum(otp_capacity) :: float / sum(sum(otp_capacity)) over () AS otp_capacity,
        100 * sum(avg_daily_enrollment_2019) :: float / sum(sum(avg_daily_enrollment_2019)) over () AS avg_daily_enrollment_2019,
        100 * sum(patient_admissions_3_2019) :: float / sum(sum(patient_admissions_3_2019)) over () AS patient_admissions_3_2019,
        100 * sum(opioid_burden_2019) :: float / sum(sum(opioid_burden_2019)) over () AS opioid_burden_2019
    FROM {{ ref('zip_code_otp_analysis') }}
    GROUP BY 1, 2
), by_uhf as (
    SELECT
        is_harlem,
        borough_name,
        100 * sum(overdose_deaths_2020) :: float / sum(sum(overdose_deaths_2020)) over () as overdose_deaths_2020
    FROM {{ ref('uhf_otp_analysis') }}
    GROUP BY 1, 2
)
, combined as (
    select
        --CASE WHEN borough_name = 'Manhattan' THEN 'Manhattan (incl. Harlem)' 
        --    ELSE borough_name END as borough_name,
        borough_name,
        --sum(total_admissions_3_2019) AS "OTP Admissions (2019)",
        sum(otp_capacity) AS "OTP Capacity (2019)",
        --sum(avg_daily_enrollment_2019) AS "OTP Daily Enrollment (2019)",
        --sum(patient_admissions_3_2019) AS "OTP Patient Population (2019)",
        --sum(opioid_burden_2019) AS "Opioid Burden (2019)",
        sum(overdose_deaths_2020) AS "Overdose Deaths (2020)",
        sum(population_2020_estimate) AS "Population (2020)"
    from by_zip
        full outer join by_uhf using (is_harlem, borough_name)
    -- where not is_harlem
    group by 1

    union

    select
        'Harlem Alone' AS borough_name,
        --sum(total_admissions_3_2019) AS "OTP Admissions (2019)",
        sum(otp_capacity) AS "OTP Capacity (2019)",
        --sum(avg_daily_enrollment_2019) AS "OTP Daily Enrollment (2019)",
        --sum(patient_admissions_3_2019) AS "OTP Patient Population (2019)",
        --sum(opioid_burden_2019) AS "Opioid Burden (2019)",
        sum(overdose_deaths_2020) AS "Overdose Deaths (2020)",
        sum(population_2020_estimate) AS "Population (2020)"
    from by_zip
        join by_uhf using (is_harlem, borough_name)
    where is_harlem
    group by 1
)
select * from combined 
order by "Overdose Deaths (2020)" desc
