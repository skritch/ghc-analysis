/*
Candace:
- Contains some null patient zip codes with nonzero counts
- Contains 1 program, 53127 "Cumberland Clinic OTP", that 
    doesn't join to `programs`
    Might match to 925 Cumberland Clinic OTP 2?
*/

WITH cleaned AS (
    SELECT
        "Program Number" as program_number,
        "Program Name" as program_name,
        "Program County" as address_county,
        "Program Category" as program_category,
        "Program Service Type" as program_service_type,
        "Program Status" as program_status,
        "Zip Code of Residence" as patient_zip_code,
        NULLIF("Total Admissions", 'R')::INT as total_admissions,
        COALESCE("Total Admissions" = 'R', True) as is_redacted
    from {{ ref('candace_foil_full') }}
)
SELECT * FROM cleaned
where 
-- removes 6 rows
patient_zip_code != '00000'
-- removes 5 rows
    and patient_zip_code != '99999'
