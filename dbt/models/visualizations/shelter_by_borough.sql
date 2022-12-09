SELECT
    shelter_population.borough_name,
    100 * sum(census_total)::float / sum(sum(census_total)) over () as "Fraction of Sheltered",
    100 * sum(adult_shelter)::float / sum(sum(adult_shelter))  over () AS "Fraction of Adult-only Sheltered",
    100 * sum(population_2020)::float / sum(sum(population_2020)) over () AS "Fraction of Population",
    100 * sum(total_individuals_associated)::float / sum(sum(total_individuals_associated)) over () AS "Fraction of Shelter Associated Address",
    100000 * sum(census_total) ::float / sum(population_2020) AS "Shelter Residents/100k",
    100000 * sum(adult_shelter) ::float / sum(population_2020) AS "Adult-only Shelter Residents/100k"
FROM {{ ref('shelter_population') }}
    JOIN {{ ref('districts') }} USING (borough_district_code)
WHERE _is_most_recent_report
GROUP BY 1