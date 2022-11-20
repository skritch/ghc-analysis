{% macro parse_zip_code(column_name) %}
   (regexp_match({{ column_name }} :: text, '[0-9]{5}$'))[1]
{% endmacro %}