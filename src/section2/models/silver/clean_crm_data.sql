-- models/marts/clean_crm_data.sql
{%- set import_crm = select_table(source('my_source', 'CRM'), ref('input_crm')) -%}
{%- set import_schema = select_schema(var('schema'),var('test_schema')) -%}
{{ config(schema=import_schema) }}
{{ log('import_crm: ' ~ import_save_table_as, info=True) }}


{#
dbt compile --vars '{"unit_testing": true}' --models models/silver/enriched_data.sql 
#}


WITH cleaned_crm AS (
    SELECT
        MSISDN,
        CASE
            WHEN LOWER(GENDER) LIKE 'f%' THEN 'Female'
            WHEN LOWER(GENDER) LIKE 'm%' THEN 'Male'
            ELSE 'Not Available'
        END AS GENDER,
        YEAR_OF_BIRTH,
        SYSTEM_STATUS,
        MOBILE_TYPE,
        VALUE_SEGMENT
    FROM {{import_crm}}
    WHERE YEAR_OF_BIRTH <= 2023
),

windowed_crm AS (
    SELECT
        *,
        LEAD(SYSTEM_STATUS) OVER (PARTITION BY MSISDN ORDER BY YEAR_OF_BIRTH, GENDER, MOBILE_TYPE, SYSTEM_STATUS, VALUE_SEGMENT) AS next_status,
        LEAD(YEAR_OF_BIRTH) OVER (PARTITION BY MSISDN ORDER BY YEAR_OF_BIRTH, GENDER, MOBILE_TYPE, SYSTEM_STATUS, VALUE_SEGMENT) AS next_yob,
        LEAD(GENDER) OVER (PARTITION BY MSISDN ORDER BY YEAR_OF_BIRTH, GENDER, MOBILE_TYPE, SYSTEM_STATUS, VALUE_SEGMENT) AS next_gender,
        LAG(SYSTEM_STATUS) OVER (PARTITION BY MSISDN ORDER BY YEAR_OF_BIRTH, GENDER, MOBILE_TYPE, SYSTEM_STATUS, VALUE_SEGMENT) AS prev_status,
        LAG(YEAR_OF_BIRTH) OVER (PARTITION BY MSISDN ORDER BY YEAR_OF_BIRTH, GENDER, MOBILE_TYPE, SYSTEM_STATUS, VALUE_SEGMENT) AS prev_yob,
        LAG(GENDER) OVER (PARTITION BY MSISDN ORDER BY YEAR_OF_BIRTH, GENDER, MOBILE_TYPE, SYSTEM_STATUS, VALUE_SEGMENT) AS prev_gender
    FROM cleaned_crm
),

flagged_crm AS (
    SELECT
        *,
        CASE
            WHEN (SYSTEM_STATUS = 'ACTIVE' AND next_status = 'SUSPEND' AND YEAR_OF_BIRTH = next_yob AND GENDER = next_gender) THEN 1
            WHEN (SYSTEM_STATUS = 'SUSPEND' AND prev_status = 'ACTIVE' AND YEAR_OF_BIRTH = prev_yob AND GENDER = prev_gender) THEN 1
            WHEN (SYSTEM_STATUS = 'ACTIVE' AND next_status = 'IDLE' AND YEAR_OF_BIRTH = next_yob AND GENDER = next_gender) THEN 1
            ELSE 0
        END AS pair_flag
    FROM windowed_crm
),

filtered_crm AS (
    SELECT
        MSISDN,
        GENDER,
        YEAR_OF_BIRTH,
        SYSTEM_STATUS,
        MOBILE_TYPE,
        VALUE_SEGMENT,
        ROW_NUMBER() OVER (PARTITION BY MSISDN ORDER BY 
            CASE SYSTEM_STATUS 
                WHEN 'ACTIVE' THEN 1 
                WHEN 'IDLE' THEN 2 
                WHEN 'SUSPEND' THEN 3 
                WHEN 'DEACTIVATE' THEN 4 
                ELSE 5 
            END,
            (CASE WHEN GENDER IS NOT NULL THEN 1 ELSE 0 END + 
            CASE WHEN YEAR_OF_BIRTH IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN SYSTEM_STATUS IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN MOBILE_TYPE IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN VALUE_SEGMENT IS NOT NULL THEN 1 ELSE 0 END) DESC
        ) AS rank
    FROM flagged_crm
    WHERE pair_flag = 0
),

crm_final AS (
    SELECT
        MSISDN,
        FIRST_VALUE(GENDER) OVER (PARTITION BY MSISDN ORDER BY rank) AS GENDER,
        FIRST_VALUE(YEAR_OF_BIRTH) OVER (PARTITION BY MSISDN ORDER BY rank) AS YEAR_OF_BIRTH,
        FIRST_VALUE(SYSTEM_STATUS) OVER (PARTITION BY MSISDN ORDER BY rank) AS SYSTEM_STATUS,
        FIRST_VALUE(MOBILE_TYPE) OVER (PARTITION BY MSISDN ORDER BY rank) AS MOBILE_TYPE,
        FIRST_VALUE(VALUE_SEGMENT) OVER (PARTITION BY MSISDN ORDER BY rank) AS VALUE_SEGMENT
    FROM filtered_crm
    WHERE rank = 1
)

SELECT DISTINCT
    MSISDN,
    GENDER,
    YEAR_OF_BIRTH,
    SYSTEM_STATUS,
    MOBILE_TYPE,
    VALUE_SEGMENT
FROM crm_final