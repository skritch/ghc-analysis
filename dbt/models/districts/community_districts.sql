select 
    borough_district_code,
    borough_district_code % 100 AS district_code,
    borough_name,
    replace(community_board_name, '  ', ' ') AS community_board_name,
    replace(community_board_name_short, '  ', ' ') AS district_name,
    population_2010,
    population_2020,
    borough_district_code in (109, 110, 111) AS is_harlem,
    ST_SetSRID(wkb_geometry::geometry, 4326) :: geography AS boundary
from {{ ref('geo_codes') }} as g
    full outer join nyc_community_district_geometries as geom 
        on geom.boro_cd = g.borough_district_code
where is_community_district