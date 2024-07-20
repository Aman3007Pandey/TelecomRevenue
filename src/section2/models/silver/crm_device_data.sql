{%- set import_schema = select_schema(var('schema'),var('test_schema')) -%}
{{ config(schema=import_schema) }}
-- Create first table with left join of CRM and Device
WITH crm AS (
    SELECT MSISDN, GENDER, YEAR_OF_BIRTH, SYSTEM_STATUS, MOBILE_TYPE, VALUE_SEGMENT 
    FROM {{ ref('clean_crm_data') }}
),
device AS (
    SELECT MSISDN, IMEI_TAC, BRAND_NAME, MODEL_NAME, OS_NAME, OS_VENDOR 
    FROM {{ ref('clean_device_data') }}
)
SELECT
    crm.MSISDN,
    crm.GENDER,
    crm.YEAR_OF_BIRTH,
    crm.SYSTEM_STATUS,
    crm.MOBILE_TYPE,
    crm.VALUE_SEGMENT,
    device.IMEI_TAC,
    device.BRAND_NAME,
    device.MODEL_NAME,
    device.OS_NAME,
    device.OS_VENDOR
FROM crm
LEFT JOIN device ON crm.MSISDN = device.MSISDN 