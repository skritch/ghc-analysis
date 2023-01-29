SELECT
    SUBSTRING("Geographic Area Name" FROM 7) as zip_code,
    "Geography" as zcta_id,
    "Estimate!!SEX AND AGE!!Total population" as population_total,
    -- Basic demographic classification, I don't know a lot about these.
    "Estimate!!RACE!!Total population!!One race!!White" as population_white,
    "Estimate!!RACE!!Total population!!One race!!Black or African American" as population_black,
    "Estimate!!RACE!!Total population!!One race!!Asian" AS population_asian,
    -- ?? Is this right?
    "Estimate!!HISPANIC OR LATINO AND RACE!!Total population" AS population_hispanic

from {{ ref('ny_zcta_demographics_raw') }}