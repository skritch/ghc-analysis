select 
    "Orig Order" as id,
    "GeoType" as geo_type, -- NYC | Boro | CD | NTA2020 | CT2020
    "Borough" AS borough_name,
    "GeoID" as geo_id,  -- boro id or district id or like BK0101 or some census ID thing
    "BCT2020" as bct_2020,  -- only defined for census tracts
    CASE WHEN COALESCE("CD Type" = 'CD', false)
        THEN "GeoID"::INT END AS borough_district_code,
    coalesce("CD Type" = 'CD', false) as is_community_district,
    "NTA Type" as nta_type_code,
    "Pop_10" AS population_2010,
    "Pop_20" AS population_2020,
    -- Only provided for CDs
    -- rename to district_name?
    trim("community board names") AS community_board_name,
    -- Which is typically used?
    trim("short community board names") as community_board_name_short
from {{ ref('geo_codes_raw') }}
-- skips parks, even though they have tiny populations
where "CD Type" = 'CD'