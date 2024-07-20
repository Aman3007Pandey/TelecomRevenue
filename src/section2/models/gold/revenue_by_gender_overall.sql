{%- set import_schema = select_schema(var('gold_schema'),var('test_gold_schema')) -%}
{{ config(schema=import_schema) }}
select gender, round(sum(revenue), 4) as revenue_by_gender_overall 
from {{ ref('enriched_data')  }}
where gender = 'Male' or gender = 'Female'
group by gender
