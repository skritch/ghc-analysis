WITH zips AS (
    SELECT
        "ZipCode" as zip_code,
        "Borough" as borough_name,
        "Neighborhood" as neighborhood_name
    FROM {{ ref('nyc_zip_codes_raw') }}
),
boroughs AS (
    SELECT DISTINCT borough_code, borough_name
    FROM {{ ref('nyc_ct_mapping') }}
)
SELECT
    zip_code,
    zips.borough_name,
    borough_code,
    zips.neighborhood_name,
    -- This isn't strictly true, but best we can do by zips
    zip_code in ('10026', '10027', '10037', '10030', '10039', '10031', '10035') AS is_harlem,
    -- Hm, '10029' maybe should be in 
    zip_code in ('10032', '10025', '10029') AS near_harlem,
    demographics.population_total,
    demographics.population_asian,
    demographics.population_black,
    demographics.population_hispanic,
    demographics.population_white,
    uhf.uhf_code,
    uhf.neighborhood_name AS uhf_name
FROM zips
    LEFT JOIN boroughs USING (borough_name)
    LEFT JOIN {{ ref('ny_zcta_demographics') }} AS demographics USING (zip_code)
    LEFT JOIN {{ ref('uhf_to_zip_raw')}} AS uhf USING (zip_code)