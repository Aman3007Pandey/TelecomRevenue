-- 2NF
CREATE TABLE IF NOT EXISTS PROJECT_SEC3_DB.PUBLIC.DEVICE_MODELS (
    MSISDN STRING NOT NULL,
    MODEL_NAME STRING NOT NULL,
    PRIMARY KEY (MSISDN, MODEL_NAME)
);


CREATE TABLE IF NOT EXISTS PROJECT_SEC3_DB.PUBLIC.DEVICE_INFO (
    MSISDN STRING NOT NULL,
    IMEI_TAC STRING NOT NULL,
    BRAND_NAME STRING,
    OS_NAME STRING,
    OS_VENDOR STRING,
    PRIMARY KEY (MSISDN, IMEI_TAC)
);


INSERT INTO project_sec3_db.public.device_models (MSISDN, MODEL_NAME)
SELECT DISTINCT MSISDN, MODEL_NAME
FROM project_sec3_db.public.NORMALIZED_DEVICE_TABLE_1NF
WHERE MSISDN IS NOT NULL AND MODEL_NAME IS NOT NULL;


-- 3426930


INSERT INTO project_sec3_db.public.device_info (MSISDN, IMEI_TAC, BRAND_NAME, OS_NAME, OS_VENDOR)
SELECT DISTINCT MSISDN, IMEI_TAC, BRAND_NAME, OS_NAME, OS_VENDOR
FROM project_sec3_db.public.NORMALIZED_DEVICE_TABLE_1NF
WHERE MSISDN IS NOT NULL AND IMEI_TAC IS NOT NULL;


-- 2440100