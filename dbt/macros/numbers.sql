{% macro parse_int_with_decimal_delim(column_name) %}
   regexp_replace({{ column_name }} :: text, '\.', '') :: int
{% endmacro %}