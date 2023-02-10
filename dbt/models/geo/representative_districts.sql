{{ config(
    indexes=[
      {'columns': ['boundary'], 'type': 'gist'},
    ]
)}}


with representatives as (
    select
        case "Office"
            When 'Representative in Congress' then 'House'
            when 'State Senator' then 'State Senate'
            when 'Member of Assembly' then 'State Assembly'
        end as district_type,
        "District"::int as district,
        "FirstName"
            || case when "MiddleName" is not null then (' ' || "MiddleName") else '' end
            || ' ' || "LastName"
            || case when "CandidateSuffix" is not null then (' ' || "CandidateSuffix") else '' end
            as representative_name,
        case "Party"
            when 'Democratic' then 'Democrat'
            when 'Republican' then 'Republican'
            else "Party" 
        end as representative_party
    from elected_officials
),
council as (
    SELECT 
        'City Council' as district_type,
        "DISTRICT" as district, 
        "NAME" as representative_name,
        "POLITICAL PARTY" as representative_party
    -- todo load this csv
    FROM nyc_city_council_members
)
, all_boundaries as (
    select
        r.*,
        ST_SetSRID(wkb_geometry::geometry, 4326) :: geography AS boundary
    from {{source('geo', 'nyc_senate_district_geometries')}} as g
        join representatives as r on district_type = 'State Senate' and r.district = g.st_sen_dis::int

    union all

    select
        r.*,
        ST_SetSRID(wkb_geometry::geometry, 4326) :: geography AS boundary
    -- Has the NYC districts only + 2 that go outside
    from {{source('geo', 'nyc_assembly_district_geometries')}} as g
        join representatives as r on district_type = 'State Assembly' and r.district = g.assem_dist::int
        
    union all

    select
        r.*,
        ST_SetSRID(wkb_geometry::geometry, 4326) :: geography AS boundary
    from {{source('geo', 'nyc_congressional_district_geometries')}} as g
        join representatives as r on district_type = 'House' and r.district = g.cong_dist::int
    where not (district in (3, 16))

    union all

    select
        council.*,
        ST_SetSRID(wkb_geometry::geometry, 4326) :: geography AS boundary
    from {{source('geo', 'nyc_city_council_district_geometries')}} as g
        join council on g.coun_dist = council.district
)
select all_boundaries.*, population as population_2020
from all_boundaries
full outer join district_populations using (district_type, district)