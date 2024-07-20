{%- set import_schema = select_schema(var('gold_schema'),var('test_gold_schema')) -%}
{{ config(schema=import_schema) }}
with total_customers as (
  select 
    count(distinct msisdn) as total_customers_overall
  from 
    {{ ref('enriched_data') }}
),
total_active_customers as (
  select 
    count(distinct msisdn) as total_active_customers_overall
  from 
    {{ ref('enriched_data') }}
  where 
    system_status = 'ACTIVE'
)
select 
  (select total_customers_overall from total_customers) as total_customers_overall,
  (select total_active_customers_overall from total_active_customers) as total_active_customers_overall
