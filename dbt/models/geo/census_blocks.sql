{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['boundary'], 'type': 'gist'},
    ]
)}}

SELECT
    b.geoid as census_block_geoid,
    -- Alt. Unique key for block
    b.bctcb2020,
    b.cb2020 as census_block_2020,
    b.borocode::int as borough_code,
    b.boroname as borough_name,
    b.shape_leng as shape_length,
    b.shape_area,
    ST_Transform(ST_SetSRID(b.wkb_geometry, 102718), 4326) :: geography AS boundary,

    -- Unique key for tract
    t.boroct2020,
    b.ct2020 as census_tract_2020,
    t.ctlabel as census_tract_label,
    t.nta2020 as nta_code,
    t.ntaname as nta_name,
    t.cdta2020 as cdta_code,
    t.cdtaname as cdta_name,

    -- Note "approximation" or "equivalent"
    ({{ parse_district_code('t.cdtaname') }}) AS district_code,
    (100 * b.borocode::int) + ({{ parse_district_code('t.cdtaname') }}) AS borough_district_code
from census_blocks_raw as b
    left join census_tracts_raw as t using (ct2020, borocode)