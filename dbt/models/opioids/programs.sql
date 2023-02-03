{{ config(
    indexes=[
      {'columns': ['location'], 'type': 'gist'},
    ]
)}}

WITH yoni_records as (
    SELECT
        program_number,
        program_name,
        program_category,
        program_service_type,
        NULL AS program_status,
        provider_number,
        provider_name,
        borough_district_code,
        address_full,
        address_county,
        COALESCE({{ parse_zip_code('address_full') }}, missing.zip_code) AS address_zip_code,
        latitude,
        longitude,
        date_operational,
        current_certified_capacity AS capacity_estimate,
        'yoni' AS _record_source
    FROM {{ ref('program_capacities_2019') }}
        LEFT JOIN {{ ref('missing_program_zip_codes') }} AS missing 
            USING (program_number)
    -- 2019 snapshot
    -- missing a lot, but has full locations
),
chan_records as (
    select distinct
        program_number,
        program_name,
        program_category,
        program_service_type,
        -- Operational, Closed, Closeout
        -- TODO: these probably change over time.
        program_status,
        provider_number,
        provider_name,
        borough_district_code,
        address_county,
        address_street || ' ' || {{ nyc_county_to_city('address_county') }} || ', ' || address_zip_code
            AS address_full,
        COALESCE(address_zip_code, missing.zip_code) AS address_zip_code,
        MAX(avg_daily_enrollment) OVER (PARTITION BY program_number) as capacity_estimate,
        'chan' as _record_source
    from {{ ref('enrollment_by_year') }}
        LEFT JOIN {{ ref('missing_program_zip_codes') }} AS missing 
            USING (program_number)
),
base AS (
    SELECT 
        program_number,
        COALESCE(f.program_name, i.program_name) AS program_name,
        COALESCE(f.program_category, i.program_category) AS program_category,
        COALESCE(f.program_service_type, i.program_service_type) AS program_service_type,
        
        -- Can't trust program_status right now
        COALESCE(f.program_status, i.program_status) AS program_status,
        COALESCE(f.provider_number, i.provider_number) AS provider_number,
        COALESCE(f.provider_name, i.provider_name) AS provider_name,
        COALESCE(f.borough_district_code, i.borough_district_code) AS borough_district_code,
        COALESCE(f.address_full, i.address_full) AS address_full,
        COALESCE(f.address_county, i.address_county) AS address_county,
        COALESCE(f.address_zip_code, i.address_zip_code) AS address_zip_code,
        f.latitude,
        f.longitude,
        date_operational,
        COALESCE(f.capacity_estimate, i.capacity_estimate) AS capacity_estimate,
        COALESCE(f._record_source, i._record_source) AS _record_source
    FROM yoni_records f
    FULL OUTER JOIN chan_records i USING (program_number)
),
combined AS (
    SELECT
        *
    FROM base

    UNION 
    -- This definitely risks having dupes if the DISTINCTs fail

    SELECT DISTINCT
        program_number,
        program_name,
        program_category,
        program_service_type,
        program_status,
        NULL :: INT AS provider_number,
        NULL AS provider_name,
        NULL :: INT AS borough_district_code,
        NULL AS address_full,
        address_county,
        NULL AS address_zip_code,
        NULL :: REAL AS latitude,
        NULL :: REAL AS longitude,
        NULL :: DATE AS date_operational,
        SUM(COALESCE(total_admissions, 3)) OVER (PARTITION BY program_number) AS capacity_estimate,
        'candace' AS _record_source
    FROM {{ ref('program_admissions_2017') }}
    WHERE program_number NOT IN (select program_number from base)


    UNION 

    SELECT DISTINCT
        program_number,
        program_name, -- One of these incorrectly has its ID for its name
        NULL AS program_category,
        program_service_type,
        NULL AS program_status,
        provider_number AS provider_number,
        provider_name,
        NULL :: INT AS borough_district_code,
        NULL AS address_full,
        NULL AS address_county,
        NULL AS address_zip_code,
        NULL :: REAL AS latitude,
        NULL :: REAL AS longitude,
        NULL :: DATE AS date_operational,
        SUM(COALESCE(total_admissions, 3)) OVER (PARTITION BY program_number) AS capacity_estimate,
        'shawn' AS _record_source
    FROM {{ ref('program_admissions_2019') }}
    WHERE program_number NOT IN (select program_number from base)
)
SELECT
    program_number,
    program_name,
    program_category,
    program_service_type,
    program_status,
    provider_number AS provider_number,
    provider_name,
    borough_district_code,
    address_full,
    address_county,
    address_zip_code,
    CASE address_county
        WHEN 'Kings' THEN 'Brooklyn'
        WHEN 'Queens' THEN 'Queens'
        WHEN 'New York' THEN 'Manhattan'
        WHEN 'Bronx' THEN 'Bronx'
        WHEN 'Richmond' THEN 'Staten Island'
    ELSE NULL END 
        AS borough_name,
    COALESCE(combined.latitude, g.latitude) AS latitude,
    COALESCE(combined.longitude, g.longitude) AS longitude,
    ST_SetSRID(ST_POINT(
        COALESCE(combined.longitude, g.longitude), 
        COALESCE(combined.latitude, g.latitude)
    ), 4326) :: GEOGRAPHY as location,
    date_operational,
    capacity_estimate,
    combined.latitude is NULL and g.latitude is not null as _is_geocoded,
    _record_source
FROM combined
    LEFT JOIN {{ref('geocoded_otps')}} AS g USING (program_number)

    
