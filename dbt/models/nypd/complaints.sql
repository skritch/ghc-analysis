{{ config(
    materialized = 'table',
    unique_key = 'complaint_id',
)}}

with raw_complaints as (
    select
        c.*,
        ST_SetSRID(ST_POINT(c.longitude, c.latitude), 4326) :: GEOGRAPHY 
            AS complaint_location
    from {{source('nypd', 'nypd_complaints')}} as c
    where latitude is not null and longitude is not null
)
select 
    c.*,
    ST_Distance(complaint_location, p.location) as distance_to_precinct_meters,
    b.bctcb2020,
    b.borough_name,
    b.borough_code,
    b.boroct2020,

    -- Note: unreliable, based on the lossy CDTA-CD conversion.
    b.borough_district_code,

    {{ categorize_arrest('offense_description', 'pd_description', 'offense_level' )}}
         AS offense_category

from raw_complaints as c
    -- join instead of left join drops only a handful of records which appear to be outside the city
    join {{ ref('census_blocks') }} as b
        on ST_Covers(b.boundary, c.complaint_location)
    left join {{ ref('nypd_precincts') }} as p on c.precinct = p.precinct