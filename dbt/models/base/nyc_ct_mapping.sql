SELECT
    "GEOID" as census_tract_id,
    "BoroCode"::int as borough_code,
    "BoroName" as borough_name,
    "NTACode" as nta_code,
    "NTAType" as nta_type,
    "NTAName" as nta_name,
    "NTAAbbrev" as nta_abbrev,
    "CDTACode" as cdta_code,
    "CDTAType" AS cdta_type,
    "CDTAName" as cdta_name,
    -- Note "approximation" or "equivalent"
    ({{ parse_district_code('"CDTAName"') }}) AS district_code,
    (100 * "BoroCode"::int) + ({{ parse_district_code('"CDTAName"') }}) AS borough_district_code
from {{ ref('nyc_ct_mapping_raw') }}