select
    uhf_name || ' (' || borough_name || ')' AS Neighborhood,
    100 * coalesce(total_admissions_3_2019, 0) :: float / sum(total_admissions_3_2019) over () as "Opioid Treatment Admissions (2019)",
    100 * coalesce(overdose_deaths_2020, 0) :: float / sum(overdose_deaths_2020) over () as "Overdose Deaths (2020)",
    100 * coalesce(opioid_burden_2019, 0) :: float / sum(opioid_burden_2019) over () as "Opioid Burden (2019)",
    100 * coalesce(patient_admissions_3_2019, 0) :: float / sum(patient_admissions_3_2019) over () as "Patient Population (2019)",
    100 * population_2020_estimate :: float / sum(population_2020_estimate) over () as "Population (2020)"
from {{ ref('uhf_otp_analysis') }}
order by 4 desc
limit 10