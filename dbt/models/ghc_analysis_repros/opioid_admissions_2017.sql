select programs.address_zip_code as zip_code,
    sum(coalesce(total_admissions, 3)) as total_patients
from {{ ref('program_admissions_2017') }} cf
    left join {{ ref('programs') }} using (program_number)
where cf.program_category = 'Opioid Treatment Program'
group by 1