{%- set import_schema = select_schema(var('gold_schema'),var('test_gold_schema')) -%}
{{ config(schema=import_schema) }}
with total_devices_week as (
    select week_number, count(distinct msisdn, imei_tac) as total_devices_weekwise
    from {{ ref('enriched_data')  }}
    where imei_tac is not null
    group by week_number
),
total_active_devices_week as (
  select week_number, count(distinct msisdn, imei_tac) as total_active_devices_weekwise
    from {{ ref('enriched_data')  }}
    where system_status = 'ACTIVE' and imei_tac is not null
    group by week_number
)
select 
    tdw.week_number,
    tdw.total_devices_weekwise as total_devices,
    tadw.total_active_devices_weekwise as total_active_devices
from 
    total_devices_week tdw
left join 
    total_active_devices_week tadw
on 
    tdw.week_number = tadw.week_number
order by 
    tdw.week_number
