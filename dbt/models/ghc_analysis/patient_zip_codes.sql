with cleaned as (
    select 
        "Zip Code" as patient_zip_code,
        ("OTP Admissions" = 'Yes') AS has_otp_patient
    from {{ ref('data-Um233') }}
)
select *
from cleaned
where patient_zip_code is not null
    and patient_zip_code != '83'

