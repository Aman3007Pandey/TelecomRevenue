{%- set import_schema = select_schema(var('gold_schema'),var('test_gold_schema')) -%}
{{ config(schema=import_schema) }}
select week_number , os_name , count(distinct msisdn) as total_customers
from {{ ref('enriched_data') }}
where os_name is not null
group by week_number , os_name
order by week_number , os_name