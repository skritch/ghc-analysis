select 
    program_number,
    program_name,
    program_category,
    service_type as program_service_type,
    provider_number,
    provider_name,

    source_borocd as borough_district_code,
    full_address as address_full,
    latitude,
    longitude,
    program_county as address_county,

    current_certified_capacity,
    COALESCE(capacity_lift_indicator = 'TRUE', false) AS has_capacity_lift,
    TO_DATE(date_operational, 'FMMM/FMDD/YYYY') AS date_operational
from {{ ref('yoni_2019_foil_raw') }}
