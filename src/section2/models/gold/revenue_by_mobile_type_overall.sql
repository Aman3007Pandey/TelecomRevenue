{%- set import_schema = select_schema(var('gold_schema'),var('test_gold_schema')) -%}
{{ config(schema=import_schema) }}
select mobile_type, round(sum(revenue),4) as revenue_by_mobile_type_overall
from {{ ref('enriched_data')  }} 
where mobile_type is not null 
group by mobile_type