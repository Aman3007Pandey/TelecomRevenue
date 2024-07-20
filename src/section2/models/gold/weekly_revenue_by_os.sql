{%- set import_schema = select_schema(var('gold_schema'),var('test_gold_schema')) -%}
{{ config(schema=import_schema) }}
SELECT week_number, os_name,
    MAX(round(revenue,4)) AS highest_revenue,
    MIN(round(revenue,4)) AS lowest_revenue
FROM {{ ref('enriched_data')  }}
GROUP BY week_number, os_name
ORDER BY week_number, os_name
