select 
    borough_district_code,
    borough_name,
    replace(community_board_name, '  ', ' ') AS community_board_name,
    replace(community_board_name_short, '  ', ' ') AS district_name,
    population_2010,
    population_2020,
    borough_district_code in (109, 110, 111) AS is_harlem
from {{ ref('geo_codes') }} as g
where is_community_district