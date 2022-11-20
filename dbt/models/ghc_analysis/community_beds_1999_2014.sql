select 
    "Community" as boro_name_short,
    "District" as borough_district_code, -- TOOD split up ampersands
    "Population,1999" as population_1999,  -- TODO pivot?
    "1999 Beds" as beds_1999,
    "Population,2014" as population_2014,
    "2014 Beds" as beds_2014
from {{ ref('data_community_beds_1999_2014') }}
