-- Override base DBT behavior to build all models directly in their custom schema,
-- rather than appending "public_" to everything.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name or target.schema }}
{%- endmacro %}
