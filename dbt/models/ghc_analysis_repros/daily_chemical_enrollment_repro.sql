-- SQL reproducing seed data-kw8ZS
WITH enrollments AS (
    SELECT  
        year,
        borough_district_code,
        district_name,
        SUM(avg_daily_enrollment) AS enrollment
    FROM {{ ref('chan_foil') }}
    LEFT JOIN {{ ref('districts') }} using (borough_district_code)
    WHERE year IN ('2010', '2019')
        AND program_category != 'Residential'
    GROUP BY 1, 2, 3
)
SELECT
    borough_district_code,
    COALESCE(
        enrollments_2010.district_name, 
        enrollments_2019.district_name
    ) AS district_name,
    enrollments_2010.enrollment as enrollment_2010,
    enrollments_2019.enrollment as enrollment_2019,
    (enrollments_2019.enrollment - enrollments_2010.enrollment) AS delta_2010_2019
FROM enrollments AS enrollments_2010
FULL OUTER JOIN enrollments AS enrollments_2019
    USING (borough_district_code)
WHERE enrollments_2010.year = '2010' AND enrollments_2019.year = '2019'
ORDER BY 1