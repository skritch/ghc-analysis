
with vector_of_125th as (
    select 
        lat0,
        lon0,
        (lat1 - lat0) 
            / sqrt((lat1 - lat0) * (lat1 - lat0) + (lon1 - lon0) * (lon1 - lon0))
            as unit_x,
        (lon1 - lon0) 
            / sqrt((lat1 - lat0) * (lat1 - lat0) + (lon1 - lon0) * (lon1 - lon0))
            as unit_y
    from (
        select 
            40.801739 as lat0,
            -73.93122 as lon0,
            40.811344 as lat1,
            -73.95399 as lon1
    ) pts
),
arrests_by_avenue_month as (
    select
        date_part('year', arrest_date) as year,
        offense_category,
        avenue_number,
        ST_Y(arrest_location::geometry) as lat,
        ST_X(arrest_location::geometry) as lon,
        bctcb2020,
        east_edge,
        south_edge
    from arrests
        join blocks_along_125 as b using (bctcb2020)
    where b.south_edge in (124, 125)
        and date_part('month', arrest_date) <= 9
)
select
    (lat - lat0) * unit_x * (lon - lon0) * unit_y
        as distance_along_125th_st,
    lat,
    lon,
    avenue_number,
    offense_category,
    year,
    bctcb2020,
    east_edge,
    south_edge
from arrests_by_avenue_month,
    vector_of_125th
where year = 2022 and offense_category = 'Major'
