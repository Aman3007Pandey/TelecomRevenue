{%- set import_schema = select_schema(var('gold_schema'),var('test_gold_schema')) -%}
{{ config(schema=import_schema) }}
with total_devices as (
  select 
    count(distinct msisdn, imei_tac) as total_devices_overall
  from 
    {{ ref('enriched_data')  }}
  where 
    imei_tac is not null
),
total_active_devices as (
  select 
    count(distinct msisdn, imei_tac) as total_active_devices_overall
  from 
    {{ ref('enriched_data')  }}
  where 
    system_status = 'ACTIVE'
    and imei_tac is not null
)
select 
  (select total_devices_overall from total_devices) as total_devices_overall,
  (select total_active_devices_overall from total_active_devices) as total_active_devices_overall
