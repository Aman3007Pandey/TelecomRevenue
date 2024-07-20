{%- set import_schema = select_schema(var('gold_schema'),var('test_gold_schema')) -%}
{{ config(schema=import_schema) }}
select week_number, value_segment, round(sum(revenue),4) as revenue_by_value_segment_weekwise
from {{ ref('enriched_data')  }} 
where value_segment is not null
group by week_number,value_segment
order by week_number,value_segment