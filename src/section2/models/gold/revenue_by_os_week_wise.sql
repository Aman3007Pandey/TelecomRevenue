{%- set import_schema = select_schema(var('gold_schema'),var('test_gold_schema')) -%}
{{ config(schema=import_schema) }}
select week_number, os_name, round(sum(revenue),4) as revenue_by_os_name_weekwise
from {{ ref('enriched_data')  }} 
where os_name is not null
group by week_number,os_name
order by week_number,revenue_by_os_name_weekwise