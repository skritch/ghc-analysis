-- SQL reproducing seed data-PnjOn

SELECT
    borough_district_code,
    district_name,
    SUM(current_certified_capacity) AS capacity
FROM {{ref('program_capacities_2019')}}
    JOIN {{ref('districts')}} using (borough_district_code)
WHERE program_category IN ('Opioid Treatment Program')
GROUP BY 1, 2
ORDER BY 1
