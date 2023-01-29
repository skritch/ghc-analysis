
with census as (
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
addresses as (
    select 
        DATE_TRUNC('month', TO_DATE("Report Date", 'MM/DD/YYYY'))::date AS report_date,
        "Borough" as borough_name,
        NULLIF("Community District", 'Unknown CD')::INT as district_code,
        "Cases" as total_cases_associated,
        "Individuals" as total_individuals_associated
    from {{ ref('shelter_cases') }}
),
buildings as (
    select 
        DATE_TRUNC('month', TO_DATE("Report Date", 'MM/DD/YYYY'))::date AS report_date,
        "Borough" as borough_name,
        "Community District" as borough_district_code,
        coalesce("Adult Family Comm Hotel", 0) as adult_family_commercial_hotel,
        coalesce("Adult Family Shelter", 0) as adult_family_shelter,
        coalesce("Adult Shelter", 0) as adult_shelter,
        coalesce("Adult Shelter Comm Hotel", 0) as adult_shelter_commercial_hotel,
        coalesce("FWC Cluster", 0) as family_cluster,
        coalesce("FWC Comm Hotel", 0) as family_children_commercial,
        coalesce("FWC Shelter", 0) as family_with_children_shelter
    from {{ ref('shelter_buildings') }}
)
SELECT
    report_date,
    report_date = max(report_date) over () as _is_most_recent_report,
    c.report_date is not null as _has_census,
    a.report_date is not null as _has_address,
    coalesce(c.borough_name, d.borough_name, a.borough_name, b.borough_name) as borough_name,
    d.borough_district_code,

    c.adult_family_commercial_hotel,
    c.adult_family_shelter,
    c.adult_shelter,
    c.adult_shelter_commercial_hotel,
    c.adult_shelter + c.adult_shelter_commercial_hotel AS adult_total,
    c.adult_shelter + c.adult_shelter_commercial_hotel + c.adult_family_commercial_hotel + c.adult_family_shelter AS adult_family_total,
    c.family_cluster,
    c.family_children_commercial,
    c.family_with_children_shelter,
    c.adult_family_commercial_hotel + c.adult_family_shelter
        + c.adult_shelter + c.adult_shelter_commercial_hotel
        + c.family_cluster
        + c.family_children_commercial + c.family_with_children_shelter AS census_total,

    b.adult_shelter as adult_shelter_buildings,
    b.adult_family_commercial_hotel + b.adult_family_shelter
        + b.adult_shelter + b.adult_shelter_commercial_hotel
        + b.family_cluster
        + b.family_children_commercial + b.family_with_children_shelter AS total_buildings,

    a.total_cases_associated,
    a.total_individuals_associated
from census as c
    full outer join addresses as a using (report_date, borough_name, district_code)
    left join {{ ref('community_districts') }} d using (borough_name, district_code)
    full outer join buildings as b using (report_date, borough_district_code)