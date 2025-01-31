{%- set import_schema = select_schema(var('gold_schema'),var('test_gold_schema')) -%}
{{ config(schema=import_schema) }}
select week_number, mobile_type ,count(distinct msisdn) as total_customers
from {{ ref('enriched_data') }}
where mobile_type is not null
group by week_number , mobile_type
order by week_number , total_customers desc