with t as (
    select
        date_trunc('month', arrest_date) as month,
        ct.borough_district_code,
        sum(
            (
                offense_description in ('BURGLAR''S TOOLS', 'BURGLARY', 'CRIMINAL TRESPASS', 'GRAND LARCENY', 'GRAND LARCENY OF MOTOR VEHICLE', 'OTHER OFFENSES RELATED TO THEF', 'OTHER OFFENSES RELATED TO THEFT', 'PETIT LARCENY', 'POSSESSION OF STOLEN PROPERTY', 'POSSESSION OF STOLEN PROPERTY 5', 'THEFT OF SERVICES')
            )::int
        ) as "Property Arrests",
        sum(
            (
                offense_description in ('CRIMINAL MISCHIEF & RELATED OF', 'CRIMINAL MISCHIEF & RELATED OFFENSES', 'DANGEROUS WEAPONS', 'DISORDERLY CONDUCT', 'HARASSMENT', 'HARRASSMENT 2', 'OFF. AGNST PUB ORD SENSBLTY &', 'OFF. AGNST PUB ORD SENSBLTY & RGHTS TO PRIV', 'OFFENSES AGAINST PUBLIC SAFETY')
            )::int
        ) as "Disorder Arrests",
        sum(
            (
                offense_description in ('DANGEROUS DRUGS', 'LOITERING FOR DRUG PURPOSES')
            )::int
        ) as "Drug Arrests",
        sum(
            (
                offense_description in ('FELONY ASSAULT', 'ARSON', 'KIDNAPPING', 'KIDNAPPING & RELATED OFFENSES', 'MURDER & NON-NEGL. MANSLAUGHTE', 'MURDER & NON-NEGL. MANSLAUGHTER', 'RAPE', 'ROBBERY', 'SEX CRIMES', 'HOMICIDE-NEGLIGENT,UNCLASSIFIE', 'HOMICIDE-NEGLIGENT,UNCLASSIFIED')
            )::int
        ) as "Serious Arrests",
        sum((offense_level = 'F')::int) as "Felony Arrests"
    from {{ ref('arrests') }}
        join {{ ref('census_tracts') }} as ct using (boroct2020)
    group by 1, 2
)
select 
    month,
    borough_district_code,
    district_name,
    population_2020 AS district_population, 
    "Property Arrests",
    1000 * "Property Arrests"::float / population_2020 AS "Property Arrests per 1k",
    "Disorder Arrests",
    1000 * "Disorder Arrests"::float / population_2020 AS "Disorder Arrests per 1k",
    "Drug Arrests",
    1000 * "Drug Arrests"::float / population_2020 AS "Drug Arrests per 1k",
    "Serious Arrests",
    1000 * "Serious Arrests"::float / population_2020 AS "Serious Arrests per 1k",
    "Felony Arrests",
    1000 * "Felony Arrests"::float / population_2020 AS "Felony Arrests per 1k"
from t
    join {{ ref('districts') }} as d using (borough_district_code)
