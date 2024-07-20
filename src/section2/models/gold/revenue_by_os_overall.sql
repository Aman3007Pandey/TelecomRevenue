{%- set import_schema = select_schema(var('gold_schema'),var('test_gold_schema')) -%}
{{ config(schema=import_schema) }}
select os_name, round(sum(revenue),4) as revenue_by_os_name_overall
from {{ ref('enriched_data')  }} 
where os_name is not null 
group by os_name
order by revenue_by_os_name_overall desc