-- SQL reproducing seed data-PnjOn

SELECT
    borough_district_code,
    district_name,
    SUM(current_certified_capacity) AS capacity
FROM {{ref('yoni_foil')}}
    JOIN {{ref('districts')}} using (borough_district_code)
WHERE program_category IN ('Opioid Treatment Program')
GROUP BY 1, 2
ORDER BY 1
