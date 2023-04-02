
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

), geolocated as (
    select
        program_number,
        district_type, 
        district
    from all_districts as r
        join {{ ref('programs') }} as p
            on ST_Intersects(r.boundary::geometry, p.location::geometry)
)
select
    p.program_number,
    g1.district as assembly_district,
    g2.district as state_senate_district,
    g3.district as city_council_district,
    g4.district as house_district,
    g5.district as community_district
from {{ ref('programs') }} as p
    left join geolocated as g1 on p.program_number = g1.program_number
        and g1.district_type = 'State Assembly'
    left join geolocated as g2 on p.program_number = g2.program_number
        and g2.district_type = 'State Senate'
    left join geolocated as g3 on p.program_number = g3.program_number
        and g3.district_type = 'City Council'
    left join geolocated as g4 on p.program_number = g4.program_number
        and g4.district_type = 'House'
    left join geolocated as g5 on p.program_number = g5.program_number
        and g5.district_type = 'Community District'
