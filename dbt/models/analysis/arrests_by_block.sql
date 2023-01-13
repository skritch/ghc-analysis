/*
Spreading arrests within 100M of the precinct.
1. Calculate the Precinct containing each Census Block
    * Just check centroid, should be faster
        * Still slow... is index working?
2. Calculate the fraction of the total area of the precinct
   which is in the intersection of each CB and Precinct
    * Or, just the fraction of area in the CB—so CBs on the edge
        round off more or less
4. Count arrests by CB
3. For CBs intersecting a 100M radius of the precinct:
    * Assign their arrest counts to the mean density
    * Spread the remainder over the entire precinct in proportion
        to the area from (2)
*/

with blocks as (
    select
        bctcb2020,
        precinct,
        ST_Area(c.boundary) as block_area
        -- overlaps?
    from {{ ref('census_blocks') }} as c
        join {{ ref('nypd_precincts') }} as p
            -- Too slow to calculate all intersections (is index working?)
            -- So just check centroid. Should be fine.
            on ST_Covers(p.boundary, c.centroid)
),
arrests_by_block as (
    select
        bctcb2020,
        precinct,
        coalesce(bool_or(distance_to_precinct_meters < 100), false) as near_precinct,
        sum(
            (offense_category in ('Property', 'Disorder', 'Drugs', 'Major'))::int
        ) as block_arrests
    from blocks
        left join arrests using (bctcb2020, precinct)
    -- todo: filter maybe
    group by 1, 2
),
precinct_totals as (
    select
        precinct,
        sum(block_area) as precinct_area,
        sum(block_arrests) as precinct_arrests,
        sum(block_arrests)::float / sum(block_area) as avg_arrest_density
    from blocks
        -- a block will only be in one precinct in blocks
        left join arrests_by_block using (bctcb2020, precinct)
    group by 1
), surpluses as (
    select
        bctcb2020,
        precinct,
        block_arrests,
        near_precinct,
        case 
            when near_precinct and block_arrests > (block_area * avg_arrest_density)
                then block_arrests - (block_area * avg_arrest_density)
            else 0
        end as surplus_arrests,
        -- surpluses from blocks near precinct will be spread over over all other blocks
        case 
            when near_precinct and block_arrests > (block_area * avg_arrest_density)
                then 0
            else block_arrests::float / precinct_arrests 
        end as fraction_of_surplus_by_arrests,

        case
            when near_precinct and block_arrests > (block_area * avg_arrest_density)
                then 0
            else block_area::float / precinct_area
        end as fraction_of_surplus_by_area
    from blocks
        join precinct_totals using (precinct)
        left join arrests_by_block using (bctcb2020, precinct)
)
select
    bctcb2020,
    precinct, 
    near_precinct,
    block_arrests as raw_arrests,
    block_arrests  
        + (fraction_of_surplus_by_arrests * sum(surplus_arrests) over (partition by precinct))
        - surplus_arrests 
            as adjusted_arrests,

    block_arrests  
        + (fraction_of_surplus_by_area * sum(surplus_arrests) over (partition by precinct))
        - surplus_arrests 
            as adjusted_arrests_by_area
from surpluses




