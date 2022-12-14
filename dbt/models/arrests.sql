{{ config(
    materialized = 'table',
    unique_key = 'address_key',
)}}

with arrests as (
    select
        a.*,
        ST_SetSRID(ST_POINT(a.longitude, a.latitude), 4326) :: GEOGRAPHY 
            AS arrest_location
    from nypd_arrests as a
)
select 
    a.*,
    b.bctcb2020,
    b.borough_name,
    b.borough_code,
    b.boroct2020,

    -- Note: unreliable, based on the lossy CDTA-CD conversion.
    b.borough_district_code
from arrests as a
    -- join instead of left join drops only a handful of records which appear to be outside the city
    join {{ ref('census_blocks') }} as b
        on ST_Covers(b.bounding_geography, a.arrest_location)