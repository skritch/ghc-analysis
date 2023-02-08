

{% macro metric_with_rank_and_percent(metric_col, metric_name, partition_by, round_to=none) %}
{% if round_to is not none  %} round(({{metric_col}})::numeric, {{round_to}}) {% else %} {{metric_col}} {% endif %}
    as "{{metric_name}}",
case when {{metric_col}} > 0 
    then rank() over (partition by {{partition_by}} order by {{metric_col}} desc)::text || '/' || count(*) over (partition by {{partition_by}})::text
else null end
    as "{{metric_name + ', rank in city'}}",
round(100 * ({{metric_col}}::float / sum({{metric_col}}) over (partition by {{partition_by}}))::numeric, 1) 
    as "{{metric_name + ', % of city'}}"
{% endmacro %}



{% macro metric_with_rank(metric_col, metric_name, partition_by, round_to=none) %}
{% if round_to is not none  %} round(({{metric_col}})::numeric, {{round_to}}) {% else %} {{metric_col}} {% endif %}
    as "{{metric_name}}",
case when {{metric_col}} > 0 
    then rank() over (partition by {{partition_by}} order by {{metric_col}} desc)::text || '/' || count(*) over (partition by {{partition_by}})::text
else null end
    as "{{metric_name + ', rank in city'}}"
{% endmacro %}