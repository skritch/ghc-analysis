{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['boundary'], 'type': 'gist'},
      {'columns': ['centroid'], 'type': 'gist'},
    ]
)}}

with geom as (
    select 
        -- Unique key for tract
        b.geoid as census_block_geoid,
        -- Alt. Unique key for block
        b.bctcb2020,
        b.cb2020 as census_block_2020,
        b.borocode::int as borough_code,
        b.boroname as borough_name,
        b.shape_leng as shape_length,
        b.shape_area,
        ST_Transform(ST_SetSRID(b.wkb_geometry, 102718), 4326) :: geography AS boundary,
        b.ct2020 as census_tract_2020
    from {{source('geo', 'census_block_geometries')}} as b
)
SELECT
    g.*,
    ST_Centroid(boundary) AS centroid,
    ST_Y(ST_Centroid(boundary)::geometry) AS centroid_latitude,
    ST_X(ST_Centroid(boundary)::geometry) AS centroid_longitude,
    t.boroct2020,
    t.ctlabel as census_tract_label,
    t.nta2020 as nta_code,
    t.ntaname as nta_name,
    t.cdta2020 as cdta_code,
    t.cdtaname as cdta_name,

    -- Note "approximation" or "equivalent"
    ({{ parse_district_code('t.cdtaname') }}) AS district_code,
    (100 * borough_code) + ({{ parse_district_code('t.cdtaname') }}) AS borough_district_code
from geom as g
    left join {{source('geo', 'census_tract_geometries')}} as t
        on g.census_tract_2020 = t.ct2020 and g.borough_code = t.borocode :: int
