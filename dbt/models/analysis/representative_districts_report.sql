with districts_and_more as (
    select
        *
    from {{ ref('representative_districts_analysis') }}

    union all

    select
    
        'Borough',
        borough_code as district,
        borough_name,
        null,
        null,
        sum(r.population_2020) as population_2020,
        sum(patient_admissions_3_2019) as patient_admissions_3_2019,
        sum(harlem_patient_admissions_3_2019) as harlem_patient_admissions_3_2019,
        sum(opioid_burden_2019) as opioid_burden_2019,
        sum(overdose_deaths_2020) as overdose_deaths_2020,
        sum(ct_otp_programs) as ct_otp_programs,
        sum(total_admissions_3_2019) as total_admissions_3_2019,
        sum(otp_capacity) as otp_capacity,
        sum(avg_daily_enrollment_2019) as avg_daily_enrollment_2019

    from {{ ref('representative_districts_analysis') }} r
        join community_districts on r.district = borough_district_code
    where district_type = 'Community District'
    group by 2, 3
)
select
    district_type,
    district,
    party,
    name,
    population_2020 
        as "District Population, 2020",
    round(100 * (population_2020::float / sum(population_2020) over (partition by district_type))::numeric, 1) 
        as "District Population, 2020, % of city",

    {{metric_with_rank_and_percent('patient_admissions_3_2019', 'OTP Patients, 2019 estimate', 'district_type') }},
    {{metric_with_rank_and_percent('1000 * patient_admissions_3_2019 / population_2020', 'OTP Patients per 1000 Population, 2019', 'district_type', 1) }},
    {{metric_with_rank_and_percent('harlem_patient_admissions_3_2019', 'Harlem OTP Admissions of Patients in District, 2019 estimate', 'district_type', 0) }},
    {{metric_with_rank_and_percent('opioid_burden_2019', 'Opioid Burden, 2019 estimate', 'district_type', 1) }},
    {{metric_with_rank_and_percent('overdose_deaths_2020', 'Overdose Deaths, 2020 estimate', 'district_type', 0) }},
    {{metric_with_rank_and_percent('100000 * overdose_deaths_2020 / population_2020', 'Overdose Deaths per 100k Population, 2020 estimate', 'district_type', 1) }},
    {{metric_with_rank_and_percent('ct_otp_programs', 'Opioid Treatment Programs, 2019', 'district_type', 0) }},
    {{metric_with_rank_and_percent('total_admissions_3_2019', 'Patients Admitted to OTP Programs in District, 2019', 'district_type', 0) }},
    {{metric_with_rank_and_percent('otp_capacity', 'OTP Capacity, 2019', 'district_type', 0) }},
    {{metric_with_rank_and_percent('avg_daily_enrollment_2019', 'Avg. Daily OTP Enrollment in District, 2019', 0) }},
    {{metric_with_rank_and_percent('1000 * avg_daily_enrollment_2019 / population_2020', 'OTP Enrollment per 1000 Population, 2019', 'district_type', 1) }}

from districts_and_more