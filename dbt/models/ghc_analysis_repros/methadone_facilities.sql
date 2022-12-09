/*
Reproduces "methadone_facilities" seed, except the CSV has:
* "Exodus" which is null anyway
* Beth Israel seems to appeaer twice in that csv; 
  one in East Harlem (1850 capacity), one in central with 800
* Odyseey House "hell gate circle" addr is split into 2 records in the CSV, vs 1 in upstream data
*/

select 
    provider_name,
    program_category,
    program_service_type,
    borough_district_code,
    district_name,
    address_full,
    sum(current_certified_capacity) AS capacity_certified
from {{ref('program_capacities_2019')}}
    join {{ref('districts')}} using (borough_district_code)
where borough_district_code in (110, 111) -- East and Central Harlem
group by 1, 2, 3, 4, 5, 6
order by provider_name

