SELECT
    uhf_code,
    uhf_name,
    uhf_code in (302, 303) AS is_harlem,
    borough_name,
    ST_Transform(ST_SetSRID(ST_Union(wkb_geometry), 102718), 4326) :: geography AS boundary,
    -- compare with some other source of population?
    -- https://www.prisonpolicy.org/origin/ny/uhf_districts.html
    sum(population_2020) AS population_2020,
    sum(population_asian) AS population_asian,
    sum(population_black) AS population_black,
    sum(population_hispanic) AS population_hispanic,
    sum(population_white) AS population_white
FROM {{ ref('zip_codes') }} as z
    FULL OUTER JOIN {{source('geo', 'uhf_geometries')}} as u ON z.uhf_code = u.uhfcode
-- drops 2 manhattan zip codes, 10065 and 10075
WHERE uhf_code IS NOT NULL
GROUP BY 1, 2, 3, 4