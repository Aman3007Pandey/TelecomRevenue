{%- set import_schema = select_schema(var('gold_schema'),var('test_gold_schema')) -%}
{{ config(schema=import_schema) }}
select os_vendor, round(sum(revenue),4) as revenue_by_os_vendor_overall
from {{ ref('enriched_data') }} 
where os_vendor is not null 
group by os_vendor
order by revenue_by_os_vendor_overall desc