
with all_districts as (
    select 
        district_type, 
        district,
        boundary
    from {{ref('representative_districts')}} 

    union all

    select 
        'UHF' as district_type, 
        uhf_code as district,
        boundary
    from {{ref('uhfs')}} 

    union all

    select 
        'Community District' as district_type, 
        borough_district_code as district,
        boundary
    from {{ref('community_districts')}} 

), o as (
    select
        district_type, 
        district,
        zip_code,
        ST_Area(z.boundary) as zip_area,
        ST_Area(ST_Intersection(r.boundary, z.boundary)) as intersection_area
    -- Note: current districts include water areas, zips don't
    -- So in the end we'll define districtarea = sum of zip areas
    from all_districts as r
        join {{ref('zip_codes')}} as z
            on ST_Intersects(r.boundary::geometry, z.boundary::geometry)
)
, district_areas as (
    select 
        district_type, 
        district,
        sum(intersection_area) as district_area,
        -- a measure of how reliably the district total can be derived from the zips
        -- 0 = perfect, 1 = bad
        -- = intersection_weighted_avg (f)
        sum(
            intersection_area * (1 - intersection_area / zip_area)) 
        / sum(intersection_area) as accuracy
    from o
    group by 1, 2
)
select
    district_type, 
    district,
    zip_code,
    intersection_area / district_area as f_of_district_area,
    intersection_area / zip_area as f_of_zip_area,
    accuracy
from o
    join district_areas using (district_type, district)
order by f_of_district_area desc