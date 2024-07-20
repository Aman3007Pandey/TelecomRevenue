{%- set import_schema = select_schema(var('gold_schema'),var('test_gold_schema')) -%}
{{ config(schema=import_schema) }}
WITH AgeSegments AS (
    SELECT 
        msisdn,
        FLOOR((YEAR(CURRENT_DATE) - year_of_birth) / 10) * 10 AS age_segment
    FROM {{ ref('enriched_data')  }}
)
SELECT 
    CASE 
        WHEN age_segment >=  0 AND age_segment < 10 THEN '0-9'
        WHEN age_segment >= 10 AND age_segment < 20 THEN '10-19'
        WHEN age_segment >= 20 AND age_segment < 30 THEN '20-29'
        WHEN age_segment >= 30 AND age_segment < 40 THEN '30-39'
        WHEN age_segment >= 40 AND age_segment < 50 THEN '40-49'
        WHEN age_segment >= 50 AND age_segment < 60 THEN '50-59'
        WHEN age_segment >= 60 AND age_segment < 70 THEN '60-69'
        WHEN age_segment >= 70 AND age_segment < 80 THEN '70-79'
        WHEN age_segment >= 80 AND age_segment < 90 THEN '80-89'
        WHEN age_segment >= 90 AND age_segment < 100 THEN '90-99'
        WHEN age_segment >= 100 THEN '>=100'
        ELSE 'Unknown'
    END AS age_range,
    ed.brand_name,
    COUNT(*) AS brand_count
FROM AgeSegments AS a
JOIN {{ref('enriched_data') }} AS ed ON a.msisdn = ed.msisdn
WHERE brand_name is not null
GROUP BY age_range, ed.brand_name
ORDER BY age_range, ed.brand_name