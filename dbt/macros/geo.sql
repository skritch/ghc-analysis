{% macro parse_district_code(column_name) %}
   (regexp_match({{ column_name }} :: text, '\(CD (\d+)'))[1]::int
{% endmacro %}

{% macro nyc_county_to_borough(county_name) %}
   CASE {{ county_name }}
        WHEN 'Kings' THEN 'Brooklyn'
        WHEN 'Queens' THEN 'Queens'
        WHEN 'New York' THEN 'Manhattan'
        WHEN 'Bronx' THEN 'Bronx'
        WHEN 'Richmond' THEN 'Staten Island'
    END
{% endmacro %}

{% macro nyc_county_to_city(county_name) %}
   CASE {{ county_name }}
        WHEN 'Kings' THEN 'Brooklyn'
        WHEN 'Queens' THEN 'Queens'
        WHEN 'New York' THEN 'New York'
        WHEN 'Bronx' THEN 'Bronx'
        WHEN 'Richmond' THEN 'Staten Island'
    END
{% endmacro %}