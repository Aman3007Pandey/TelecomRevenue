{%- set import_schema = select_schema(var('gold_schema'),var('test_gold_schema')) -%}
{{ config(schema=import_schema) }}
select brand_name, round(sum(revenue),4) as revenue_by_brand_overall
from {{ ref('enriched_data') }} 
where brand_name is not null 
group by brand_name
order by revenue_by_brand_overall desc