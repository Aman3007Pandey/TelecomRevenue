{%- set import_schema = select_schema(var('gold_schema'),var('test_gold_schema')) -%}
{{ config(schema=import_schema) }}
with total_customers_week as (
    select week_number, count(distinct msisdn) as total_customers_weekwise
    from {{ ref('enriched_data') }}
    group by week_number
),
total_active_customers_week as (
  select week_number, count(distinct msisdn) as total_active_customers_weekwise
    from {{ ref('enriched_data') }}
    where system_status = 'ACTIVE'
    group by week_number
)
select 
    tcw.week_number,
    tcw.total_customers_weekwise as total_customers,
    tacw.total_active_customers_weekwise as total_active_customers
from 
    total_customers_week tcw
left join 
    total_active_customers_week tacw
on 
    tcw.week_number = tacw.week_number
order by 
    tcw.week_number
