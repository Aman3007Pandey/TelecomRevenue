DESCRIBE TABLE DEVICE_INFO;

-- name	        type	            kind	null?	primary key	    unique key	
-- MSISDN	    VARCHAR(16777216)	COLUMN	 N		     Y	             N					
-- IMEI_TAC	    VARCHAR(16777216)	COLUMN	 N		     Y	             N					
-- BRAND_NAME	VARCHAR(16777216)	COLUMN	 Y		     N	             N					
-- OS_NAME	    VARCHAR(16777216)	COLUMN	 Y		     N	             N					
-- OS_VENDOR	VARCHAR(16777216)	COLUMN	 Y		     N	             N	

DESCRIBE TABLE DEVICE_MODELS;

-- name	        type	            kind	null?	primary key	unique key
-- MSISDN	    VARCHAR(16777216)	COLUMN	N		   Y	        N					
-- MODEL_NAME	VARCHAR(16777216)	COLUMN	N		   Y	        N					

SELECT
    'IMEI_TAC' AS Attribute1,
    'BRAND_NAME' AS Attribute2,
    COUNT(DISTINCT IMEI_TAC, BRAND_NAME) AS Count
FROM device_info
UNION ALL
SELECT
    'IMEI_TAC' AS Attribute1,
    'OS_NAME' AS Attribute2,
    COUNT(DISTINCT IMEI_TAC, OS_NAME) AS Count
FROM device_info
UNION ALL
SELECT
    'IMEI_TAC' AS Attribute1,
    'OS_VENDOR' AS Attribute2,
    COUNT(DISTINCT IMEI_TAC, OS_VENDOR) AS Count
FROM device_info;


-- ATTRIBUTE1	ATTRIBUTE2	COUNT
-- IMEI_TAC	BRAND_NAME	27469
-- IMEI_TAC	OS_NAME	    26413
-- IMEI_TAC	OS_VENDOR	26544


SELECT
    'BRAND_NAME' AS Attribute1,
    'MSISDN' AS Attribute2,
    COUNT(DISTINCT BRAND_NAME, MSISDN) AS Count
FROM device_info
UNION ALL
SELECT
    'OS_NAME' AS Attribute1,
    'MSISDN' AS Attribute2,
    COUNT(DISTINCT OS_NAME, MSISDN) AS Count
FROM device_info
UNION ALL
SELECT
    'OS_VENDOR' AS Attribute1,
    'MSISDN' AS Attribute2,
    COUNT(DISTINCT OS_VENDOR, MSISDN) AS Count
FROM device_info;

-- ATTRIBUTE1	ATTRIBUTE2	COUNT
-- BRAND_NAME	MSISDN	    2397548
-- OS_NAME	    MSISDN	    2317074
-- OS_VENDOR	MSISDN	    2333637