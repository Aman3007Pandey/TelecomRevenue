{%- set import_schema = select_schema(var('gold_schema'),var('test_gold_schema')) -%}
{{ config(schema=import_schema) }}
select gender, week_number, round(sum(revenue),4) as revenue_by_gender_weekwise 
from {{ ref('enriched_data') }} 
where gender = 'Male' or gender = 'Female'
group by gender, week_number
order by week_number, gender