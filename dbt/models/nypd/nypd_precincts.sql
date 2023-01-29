{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['boundary'], 'type': 'gist'},
    ]
)}}

WITH geometries AS (
    SELECT
        precinct,
        -- shape_leng as shape_length,
        -- shape_area,
        ST_SetSRID(wkb_geometry, 4326) :: GEOGRAPHY 
            AS boundary
        FROM {{ source('nypd', 'nypd_precinct_geometries') }}
),
locations AS (

    SELECT
        precinct,
        precinct_name,
        full_address,
        borough,
        latitude,
        longitude,
        ST_SetSRID(ST_POINT(longitude, latitude), 4326) :: GEOGRAPHY AS location
    FROM {{ source('nypd', 'nypd_precinct_locations') }}
)
SELECT
    *
FROM locations
FULL OUTER JOIN geometries USING (precinct)
    