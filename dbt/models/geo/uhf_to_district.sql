
with all_districts as (
    select 
        district_type, 
        district,
        boundary
    from {{ref('representative_districts')}} 

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
        uhf_code,
        ST_Area(u.boundary) as uhf_area,
        ST_Area(ST_Intersection(r.boundary, u.boundary)) as intersection_area
    -- Note: current districts include water areas, zips don't
    -- So in the end we'll define districtarea = sum of zip areas
    from all_districts as r
        join {{ref('uhfs')}} as u
            on ST_Intersects(r.boundary::geometry, u.boundary::geometry)
)
, district_areas as (
    select 
        district_type, 
        district,
        sum(intersection_area) as district_area,
        -- a measure of how reliably the district total can be derived from the zips
        -- 0 = perfect, 1 = bad
        -- = intersection_weighted_avg (f))
        sum(
            intersection_area * (1 - intersection_area / uhf_area)) 
        / sum(intersection_area) as accuracy
    from o
    group by 1, 2
),
pops_from_zips as (
    select
        district,
        district_type,
        uhf_code,
        sum(zd.f_of_zip_area * z.population_2020) as population_2020
    from {{ref('zip_to_district')}} as zd
        join {{ref('zip_codes')}} as z using (zip_code)
    group by 1, 2, 3
)
select
    district_type, 
    district,
    uhf_code,
    intersection_area / district_area as f_of_district_area,
    intersection_area / uhf_area as f_of_uhf_area,
    p.population_2020::float / sum(p.population_2020) over (partition by district_type, uhf_code)
        as f_of_uhf_population,
    accuracy
from o
    join pops_from_zips as p using (district_type, district, uhf_code)
    join district_areas using (district_type, district)
order by f_of_district_area desc