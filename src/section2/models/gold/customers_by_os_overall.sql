{%- set import_schema = select_schema(var('gold_schema'),var('test_gold_schema')) -%}
{{ config(schema=import_schema) }}
select os_name, count(distinct msisdn) as total_customers_by_os_name
from {{ ref('enriched_data') }}
where os_name is not null
group by os_name