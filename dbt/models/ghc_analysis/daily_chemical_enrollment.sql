select 
    "District" as borough_district_code,
    regexp_replace("Community District", '\d+-', '') AS district_name,
    "2010" as enrollment_2010,
    "2019" as enrollment_2019,
    ("2019" - "2010") as delta_2010_2019
from {{ ref('data_kw8ZS') }}
where "2019" is not null
	and "2010" is not null
