select 
    "Boro CD code" as borough_district_code,
    "Borough" as borough_name,
    "TOTAL" as sheltered_total,
    coalesce({{ parse_int_with_decimal_delim('"Adult Family Commercial Hotel"') }}, 0) as sheltered_adult_family_commercial_hotel,
    coalesce({{ parse_int_with_decimal_delim('"Adult Family Shelter"') }}, 0) as sheltered_adult_family_shelter,
    coalesce({{ parse_int_with_decimal_delim('"Adult Shelter"') }}, 0) as sheltered_adult_shelter,
    coalesce({{ parse_int_with_decimal_delim('"Adult Shelter Commercial Hotel"') }}, 0) as sheltered_adult_shelter_commercial_hotel,
    coalesce({{ parse_int_with_decimal_delim('"Family Cluster"') }}, 0) as sheltered_family_cluster,
    coalesce({{ parse_int_with_decimal_delim('"Family with Children Commercial Hotel"') }}, 0) as sheltered_family_children_commercial,
    coalesce({{ parse_int_with_decimal_delim('"Family with Children Shelter"') }}, 0) as sheltered_family_with_children_shelter
from {{ ref('data_qqf5E') }}
