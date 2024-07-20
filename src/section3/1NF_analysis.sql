

DESCRIBE TABLE NORMALIZED_DEVICE_TABLE_1NF;


-- name	    type	            kind	null?	primary key	unique key
-- BRAND_NAME	VARCHAR(16777216)	COLUMN	Y		    N	        N					
-- MSISDN	    VARCHAR(16777216)	COLUMN	Y		    N	        N					
-- IMEI_TAC	VARCHAR(16777216)	COLUMN	Y		    N	        N					
-- MODEL_NAME	VARCHAR(16777216)	COLUMN	Y		    N	        N					
-- OS_NAME	    VARCHAR(16777216)	COLUMN	Y		    N	        N					
-- OS_VENDOR	VARCHAR(16777216)	COLUMN	Y		    N	        N					


SELECT
    'MSISDN' AS Attribute1,
    'BRAND_NAME' AS Attribute2,
    COUNT(DISTINCT MSISDN, BRAND_NAME) AS Count
FROM normalized_device_table_1nf
UNION ALL
SELECT
    'MSISDN' AS Attribute1,
    'IMEI_TAC' AS Attribute2,
    COUNT(DISTINCT MSISDN, IMEI_TAC) AS Count
FROM normalized_device_table_1nf
UNION ALL
SELECT
    'MSISDN' AS Attribute1,
    'MODEL_NAME' AS Attribute2,
    COUNT(DISTINCT MSISDN, MODEL_NAME) AS Count
FROM normalized_device_table_1nf
UNION ALL
SELECT
    'MSISDN' AS Attribute1,
    'OS_NAME' AS Attribute2,
    COUNT(DISTINCT MSISDN, OS_NAME) AS Count
FROM normalized_device_table_1nf
UNION ALL
SELECT
    'MSISDN' AS Attribute1,
    'OS_VENDOR' AS Attribute2,
    COUNT(DISTINCT MSISDN, OS_VENDOR) AS Count
FROM normalized_device_table_1nf
UNION ALL
SELECT
    'BRAND_NAME' AS Attribute1,
    'IMEI_TAC' AS Attribute2,
    COUNT(DISTINCT BRAND_NAME, IMEI_TAC) AS Count
FROM normalized_device_table_1nf
UNION ALL
SELECT
    'BRAND_NAME' AS Attribute1,
    'MODEL_NAME' AS Attribute2,
    COUNT(DISTINCT BRAND_NAME, MODEL_NAME) AS Count
FROM normalized_device_table_1nf
UNION ALL
SELECT
    'BRAND_NAME' AS Attribute1,
    'OS_NAME' AS Attribute2,
    COUNT(DISTINCT BRAND_NAME, OS_NAME) AS Count
FROM normalized_device_table_1nf
UNION ALL
SELECT
    'BRAND_NAME' AS Attribute1,
    'OS_VENDOR' AS Attribute2,
    COUNT(DISTINCT BRAND_NAME, OS_VENDOR) AS Count
FROM normalized_device_table_1nf
UNION ALL
SELECT
    'IMEI_TAC' AS Attribute1,
    'MODEL_NAME' AS Attribute2,
    COUNT(DISTINCT IMEI_TAC, MODEL_NAME) AS Count
FROM normalized_device_table_1nf
UNION ALL
SELECT
    'IMEI_TAC' AS Attribute1,
    'OS_NAME' AS Attribute2,
    COUNT(DISTINCT IMEI_TAC, OS_NAME) AS Count
FROM normalized_device_table_1nf
UNION ALL
SELECT
    'IMEI_TAC' AS Attribute1,
    'OS_VENDOR' AS Attribute2,
    COUNT(DISTINCT IMEI_TAC, OS_VENDOR) AS Count
FROM normalized_device_table_1nf
UNION ALL
SELECT
    'MODEL_NAME' AS Attribute1,
    'OS_NAME' AS Attribute2,
    COUNT(DISTINCT MODEL_NAME, OS_NAME) AS Count
FROM normalized_device_table_1nf
UNION ALL
SELECT
    'MODEL_NAME' AS Attribute1,
    'OS_VENDOR' AS Attribute2,
    COUNT(DISTINCT MODEL_NAME, OS_VENDOR) AS Count
FROM normalized_device_table_1nf
UNION ALL
SELECT
    'OS_NAME' AS Attribute1,
    'OS_VENDOR' AS Attribute2,
    COUNT(DISTINCT OS_NAME, OS_VENDOR) AS Count
FROM normalized_device_table_1nf;


-- ATTRIBUTE1	ATTRIBUTE2	    COUNT
-- MSISDN	    BRAND_NAME	    2397548
-- MSISDN	    IMEI_TAC	    2440100
-- MSISDN	    MODEL_NAME	    3426930
-- MSISDN	    OS_NAME	        2317074
-- MSISDN	    OS_VENDOR	    2333637
-- BRAND_NAME	IMEI_TAC	    27469
-- BRAND_NAME	MODEL_NAME	    17383
-- BRAND_NAME	OS_NAME	        1147
-- BRAND_NAME	OS_VENDOR	    1154
-- IMEI_TAC	    MODEL_NAME	    32487
-- IMEI_TAC	    OS_NAME	        26413
-- IMEI_TAC	    OS_VENDOR	    26544
-- MODEL_NAME	OS_NAME	        14742
-- MODEL_NAME	OS_VENDOR	    14857
-- OS_NAME	    OS_VENDOR	    45