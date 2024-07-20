{%- set import_schema = select_schema(var('schema'),var('test_schema')) -%}
{{ config(schema=import_schema) }}
{%- set import_rev = select_table(source('my_source', 'REV'), ref('input_rev')) -%}
{{ log('import_rev: ' ~ import_rev, info=True) }}

WITH revenue_device AS (
    SELECT 
        revenue.MSISDN,
        revenue.WEEK_NUMBER,
        revenue.REVENUE_USD,
        device.IMEI_TAC,
        device.BRAND_NAME,
        device.MODEL_NAME,
        device.OS_NAME,
        device.OS_VENDOR
    FROM {{import_rev}} AS revenue
    LEFT JOIN {{ref('clean_device_data')}} AS device
    ON revenue.MSISDN = device.MSISDN
)
SELECT 
    rd.MSISDN,
    rd.WEEK_NUMBER,
    rd.REVENUE_USD as REVENUE,
    rd.IMEI_TAC,
    rd.BRAND_NAME,
    rd.MODEL_NAME,
    rd.OS_NAME,
    rd.OS_VENDOR,
    crm.GENDER,
    crm.YEAR_OF_BIRTH,
    crm.SYSTEM_STATUS,
    crm.MOBILE_TYPE,
    crm.VALUE_SEGMENT
FROM revenue_device AS rd
LEFT JOIN {{ref('clean_crm_data')}} AS crm
ON rd.MSISDN = crm.MSISDN 