WITH base as (
    SELECT
        "Year" as year,
        "Provider No" as provider_number,
        "Provider Name" as provider_name,
        "Program No" as program_number,
        "Program Name" as program_name,
        "Program Type" as program_category,
        "Service Type" as program_service_type,
        "Current Status" as program_status,
        "Current Community District" as borough_district_code,
        "Street Address" as address_street,
        "Zip Code" as address_zip_code,
        "County" as address_county,
        "Average Daily Enrollment" as avg_daily_enrollment
    from {{ ref('chan_foil') }}
)
SELECT 
    base.*,
    address_street || ' ' || {{ nyc_county_to_city('address_county') }} || ', ' || address_zip_code AS address_full,
    g.latitude,
    g.longitude,
    g.latitude IS NOT NULL AS _is_geocoded
FROM base
-- 9 rows, with nulls for enrollment as well
    LEFT JOIN {{ ref('geocoded_otps') }} g USING (program_number)
WHERE year IS NOT NULL
