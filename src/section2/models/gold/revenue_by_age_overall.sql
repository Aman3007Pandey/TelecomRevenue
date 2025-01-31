{%- set import_schema = select_schema(var('gold_schema'),var('test_gold_schema')) -%}
{{ config(schema=import_schema) }}
select (year(current_date) - year_of_birth) AS age, round(sum(revenue),4) as revenue_by_age_overall
from {{ ref('enriched_data') }}
where age is not null
group by age
order by age