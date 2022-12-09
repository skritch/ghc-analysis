with cleaned as (
    select 
        DATE_TRUNC('month', TO_DATE("Report Date", 'MM/DD/YYYY'))::date AS report_date,
        "Borough" as borough_name,
        "Community District" as borough_district_code,
        coalesce("Adult Family Comm Hotel", 0) as adult_family_commercial_hotel_buildings,
        coalesce("Adult Family Shelter", 0) as adult_family_shelter_buildings,
        coalesce("Adult Shelter", 0) as adult_shelter_buildings,
        coalesce("Adult Shelter Comm Hotel", 0) as adult_shelter_commercial_hotel_buildings,
        coalesce("FWC Cluster", 0) as family_cluster_buildings,
        coalesce("FWC Comm Hotel", 0) as family_children_commercial_buildings,
        coalesce("FWC Shelter", 0) as family_with_children_shelter_buildings
    from shelter_buildings
)
select *
from cleaned
order by report_date desc, borough_district_code

/*
East Harlem: 9 adult, 7 family
Central: 10 adult, 9 family
West: 1 adult, 5 family

... how many can I account for?
*/