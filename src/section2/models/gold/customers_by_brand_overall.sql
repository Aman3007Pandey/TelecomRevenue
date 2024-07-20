{%- set import_schema = select_schema(var('gold_schema'),var('test_gold_schema')) -%}
{{ config(schema=import_schema) }}
select brand_name, count(distinct msisdn) as total_customers
from {{ref('enriched_data') }}
where brand_name is not null
group by brand_name
order by total_customers desc
