
{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['boundary'], 'type': 'gist'},
    ]
)}}

SELECT
    geoid as census_tract_geoid,
    -- Unique key
    boroct2020,
    ct2020 as census_tract_2020,
    borocode::int as borough_code,
    boroname as borough_name,
    nta2020 as nta_code,
    ntaname as nta_name,
    cdta2020 as cdta_code,
    cdtaname as cdta_name,
    shape_leng as shape_length,
    shape_area,
    ST_Transform(ST_SetSRID(wkb_geometry, 102718), 4326) :: geography AS boundary,

    -- Note "approximation" or "equivalent"
    ({{ parse_district_code('cdtaname') }}) AS district_code,
    (100 * borocode::int) + ({{ parse_district_code('cdtaname') }}) AS borough_district_code
from census_tracts_raw
