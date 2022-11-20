select 
    "Zip Code" as zip_code,
    "Total Patients" as total_patients,
    
    -- Remove these?
    "Lat" as latitude,
    "Lon" as longitude
from {{ ref('data-PLEoI') }}
