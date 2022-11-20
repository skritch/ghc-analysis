
select
    patient_zip_code,
    sum(coalesce(total_admissions, 3)) as total_admissions
from {{ref('shawn_foil')}}
    join {{ ref('zip_codes') }} z on patient_zip_code = z.zip_code
    left join {{ ref('programs') }} using (program_number)
where borough_district_code in (109,110,111)
    and program_category = 'Opioid Treatment Program'
group by 1
