-- models/marts/clean_device_data.sql
{%- set import_device = select_table(source('my_source', 'DEVICE'), ref('input_device')) -%}
{{ log('import_device: ' ~ import_device, info=True) }}
{%- set import_schema = select_schema(var('schema'),var('test_schema')) -%}
{{ config(schema=import_schema) }}
WITH non_null_device AS (
    SELECT
        BRAND_NAME,
        OS_NAME,
        OS_VENDOR,
        COUNT(*) AS count
    FROM {{ import_device }}
    WHERE OS_NAME IS NOT NULL AND OS_VENDOR IS NOT NULL
    GROUP BY BRAND_NAME, OS_NAME, OS_VENDOR
),

ranked_os AS (
    SELECT
        BRAND_NAME,
        OS_NAME,
        OS_VENDOR,
        ROW_NUMBER() OVER (PARTITION BY BRAND_NAME ORDER BY count DESC) AS rank
    FROM non_null_device
),

most_frequent_os AS (
    SELECT
        BRAND_NAME,
        OS_NAME AS most_frequent_os_name,
        OS_VENDOR AS most_frequent_os_vendor
    FROM ranked_os
    WHERE rank = 1
),

device_with_os AS (
    SELECT
        d.IMEI_TAC,
        d.MODEL_NAME,
        d.MSISDN,
        d.BRAND_NAME,
        COALESCE(d.OS_NAME, mfo.most_frequent_os_name) AS OS_NAME,
        COALESCE(d.OS_VENDOR, mfo.most_frequent_os_vendor) AS OS_VENDOR
    FROM {{ import_device }} d
    LEFT JOIN most_frequent_os mfo ON d.BRAND_NAME = mfo.BRAND_NAME
)

SELECT
    MSISDN,
    IMEI_TAC,
    MODEL_NAME,
    BRAND_NAME,
    OS_NAME,
    OS_VENDOR
FROM device_with_os
