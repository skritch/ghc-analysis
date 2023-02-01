

with representatives as (
    select
        case "Office"
            When 'Representative in Congress' then 'House'
            when 'State Senator' then 'State Senate'
            when 'Member of Assembly' then 'State Assembly'
        end as office,
        "District"::int as district,
        case "Party"
            when 'Democratic' then 'Democrat'
            when 'Republican' then 'Republican'
            else "Party" 
        end as party,
        "FirstName"
            || case when "MiddleName" is not null then (' ' || "MiddleName") else '' end
            || ' ' || "LastName"
            || case when "CandidateSuffix" is not null then (' ' || "CandidateSuffix") else '' end
            as name
    from elected_officials
),
council as (
    SELECT 
        'City Council' as office,
        "DISTRICT" as district, 
        "POLITICAL PARTY" as party,
        "NAME" as name
    -- todo load this csv
    FROM nyc_city_council_members
)
, all_boundaries as (
    select
        r.*,
        ST_SetSRID(wkb_geometry::geometry, 4326) :: geography AS boundary
    from nyc_senate_district_geometries as g
        join representatives as r on office = 'State Senate' and r.district = g.st_sen_dis::int

    union all

    select
        r.*,
        ST_SetSRID(wkb_geometry::geometry, 4326) :: geography AS boundary
    -- Has the NYC districts only + 2 that go outside
    from nyc_assembly_district_geometries as g
        join representatives as r on office = 'State Assembly' and r.district = g.assem_dist::int
        
    union all

    select
        r.*,
        ST_SetSRID(wkb_geometry::geometry, 4326) :: geography AS boundary
    from nyc_congressional_district_geometries as g
        join representatives as r on office = 'House' and r.district = g.cong_dist::int
    where not (district in (3, 16))

    union all

    select
        council.*,
        ST_SetSRID(wkb_geometry::geometry, 4326) :: geography AS boundary
    from nyc_city_council_district_geometries as g
        join council on g.coun_dist = council.district
)
select all_boundaries.*, population
from all_boundaries
full outer join district_populations using (office, district)