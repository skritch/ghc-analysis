select
    zip_code,
    uhf_name,
    coalesce(total_admissions_3_2019, 0) as "OTP Admissions in Zip Code",
    100 * coalesce(total_admissions_3_2019, 9)::float / sum(total_admissions_3_2019) over () as "Fraction of OTP Admissions in Zip Code",
    coalesce(patient_admissions_3_2019, 0) as "OTP Admission Patients from Zip Code",
    100 * coalesce(patient_admissions_3_2019, 0)::float / sum(patient_admissions_3_2019) over () as "Fraction of OTP Admission Patients from Zip Code",
    coalesce(harlem_patient_admissions_3_2019, 0) as "Harlem OTP Admissions from Zip Code",
    100 * coalesce(harlem_patient_admissions_3_2019, 0)::float / sum(harlem_patient_admissions_3_2019) over () as "Fraction of Harlem's OTP Admissions from Zip Code",
    100 * coalesce(harlem_patient_admissions_3_2019, 0)::float / nullif(patient_admissions_3_2019, 0) as "Fraction of Zip Code's Admissions to Harlem OTPs",
    coalesce(in_zip_admissions_3_2019, 0) as "In-Zip Patient Admissions",
    100 * coalesce(in_zip_admissions_3_2019, 0)::float / nullif(total_admissions_3_2019, 0) as "Fraction of Admissions from In-Zip Patients"
from {{ref('zip_code_otp_analysis')}}
    join {{ref('zip_codes')}} using (zip_code)
order by 7 desc nulls last 