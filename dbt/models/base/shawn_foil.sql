/**
* 48 / 10k missing a program number in shawn_foil

53287  PROMESA, Inc. CCBHC
    Promesa locs are either in Bronx 10457 or Court 11231
    All rows map to bronx
53447 + name incorrectly given as 53447
    Locs in 11420, 11432, 11580, 11691, 11414
    Patients are mostly near those but some aren't
53284 + Samaritan Daytop Village, Inc. CCBHC
    our locs: 10469 bronx, 11435 Queens, 10027 garment, 10025 UWS
    Most obviously go to bronx but 2 are vague
53290 Services for the Underserved CCBHC
    One loc 10018 manhattan? Maybe the old one closed?
    All the patients are from BK
*/

WITH cleaned as (
    SELECT
        "Provider No" as provider_number,
        "Provider Name" as provider_name,
        "Program No" as program_number,
        "Program Name" as program_name,
        -- Opioid Treatment or Outpatient Services
        "Program Type" as program_service_type,
        -- And only maps to programs.program_categories of Opioid Treatment Program, Outpatient
        "Zip Code of Residence at Admission" as patient_zip_code,
        "Admissions"::INT as total_admissions,
        "Admissions" IS NULL as is_redacted
    from {{ ref('shawn_foil_raw') }}
)
SELECT *
FROM cleaned
WHERE 
    -- removes 9 rows
    patient_zip_code != '00000'
    -- removes 5 rows
    AND patient_zip_code != '99999'
