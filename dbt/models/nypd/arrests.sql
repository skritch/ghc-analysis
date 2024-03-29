{{ config(
    materialized = 'table',
    unique_key = 'arrest_key',
)}}

with raw_arrests as (
    select
        a.*,
        ST_SetSRID(ST_POINT(a.longitude, a.latitude), 4326) :: GEOGRAPHY 
            AS arrest_location
    from {{source('nypd', 'nypd_arrests')}} as a
)
select 
    a.*,
    ST_Distance(arrest_location, p.location) as distance_to_precinct_meters,
    b.bctcb2020,
    b.borough_name,
    b.borough_code,
    b.boroct2020,

    -- Note: unreliable, based on the lossy CDTA-CD conversion.
    b.borough_district_code,

    {{ categorize_arrest('offense_description', 'pd_description', 'offense_level' )}}
        AS offense_category

from raw_arrests as a
    -- join instead of left join drops only a handful of records which appear to be outside the city
    join {{ ref('census_blocks') }} as b
        on ST_Covers(b.boundary, a.arrest_location)
    left join {{ ref('nypd_precincts') }} as p on a.precinct = p.precinct