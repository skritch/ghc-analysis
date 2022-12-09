
with base as (
    select 
        DATE_TRUNC('month', TO_DATE("Report Date", 'MM/DD/YYYY'))::date AS report_date,
        "Borough" as borough_name,
        "Community Districts" as district_code,
        --drop "census type", every row is "Individuals"
        coalesce("Adult Family Commercial Hotel", 0) as adult_family_commercial_hotel,
        coalesce("Adult Family Shelter", 0) as adult_family_shelter,
        coalesce("Adult Shelter", 0) as adult_shelter,
        coalesce("Adult Shelter Commercial Hotel", 0) as adult_shelter_commercial_hotel,
        coalesce("Family Cluster", 0) as family_cluster,
        coalesce("Family with Children Commercial Hotel", 0) as family_children_commercial,
        coalesce("Family with Children Shelter", 0) as family_with_children_shelter
    from {{ ref('shelter_census') }}
),
cases as (
    select 
        DATE_TRUNC('month', TO_DATE("Report Date", 'MM/DD/YYYY'))::date AS report_date,
        "Borough" as borough_name,
        NULLIF("Community District", 'Unknown CD')::INT as district_code,
        "Cases" as total_cases_associated,
        "Individuals" as total_individuals_associated
    from {{ ref('shelter_cases') }}
)
SELECT
    report_date,
    report_date = max(report_date) over () as _is_most_recent_report,
    b.report_date is not null as _has_census,
    c.report_date is not null as _has_cases,
    borough_name,
    d.borough_district_code,
    adult_family_commercial_hotel,
    adult_family_shelter,
    adult_shelter,
    adult_shelter_commercial_hotel,
    adult_shelter + adult_shelter_commercial_hotel AS adult_total,
    adult_shelter + adult_shelter_commercial_hotel + adult_family_commercial_hotel + adult_family_shelter AS adult_family_total,
    family_cluster,
    family_children_commercial,
    family_with_children_shelter,
    adult_family_commercial_hotel + adult_family_shelter
        + adult_shelter + adult_shelter_commercial_hotel
        + family_cluster
        + family_children_commercial + family_with_children_shelter AS census_total,
    c.total_cases_associated,
    c.total_individuals_associated
from base b
    full outer join cases c using (report_date, borough_name, district_code)
    left join districts d using (borough_name, district_code)