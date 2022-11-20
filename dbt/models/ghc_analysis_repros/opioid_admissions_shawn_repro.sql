
select programs.address_zip_code as zip_code,
    sum(coalesce(total_admissions, 3)) as total_patients
from {{ ref('shawn_foil') }}
    left join {{ ref('programs') }} using (program_number)
where program_category = 'Opioid Treatment Program'
group by 1