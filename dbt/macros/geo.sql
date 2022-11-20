{% macro parse_district_code(column_name) %}
   (regexp_match({{ column_name }} :: text, '\(CD (\d+)'))[1]::int
{% endmacro %}