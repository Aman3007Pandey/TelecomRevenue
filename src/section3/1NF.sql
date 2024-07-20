CREATE DATABASE PROJECT_SEC3_DB;

CREATE TABLE IF NOT EXISTS PROJECT_SEC3_DB.PUBLIC.DEVICE AS
SELECT *
FROM PROJECT_DB.PUBLIC.DEVICE;

DESCRIBE TABLE DEVICE;

-- 1NF
CREATE TABLE IF NOT EXISTS NORMALIZED_DEVICE_TABLE_1NF (
    BRAND_NAME STRING,
    MSISDN STRING,
    IMEI_TAC STRING,
    MODEL_NAME STRING,
    OS_NAME STRING,
    OS_VENDOR STRING
);

CREATE OR REPLACE PROCEDURE SPLIT_MODEL_NAME()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
def run(session):
    from snowflake.snowpark import Session
    from snowflake.snowpark.functions import col
    import re

    # Retrieve data from the device table
    device_data = session.table("project_sec3_db.public.device").select("BRAND_NAME", "MSISDN", "IMEI_TAC", "MODEL_NAME", "OS_NAME", "OS_VENDOR").collect()

    # Initialize an empty list to store the transformed data
    data = []

    # Iterate through the retrieved rows
    for row in device_data:
        brand_name = row['BRAND_NAME']
        msisdn = row['MSISDN']
        imei_tac = row['IMEI_TAC']
        model_name = row['MODEL_NAME']
        os_name = row['OS_NAME']
        os_vendor = row['OS_VENDOR']

        if model_name is None:
            # Handle null model_name
            data.append([brand_name, msisdn, imei_tac, None, os_name, os_vendor])
        else:
            # Split by commas outside of parentheses first and filter out empty segments
            parts = [part.strip() for part in re.split(r',(?![^()]*\))', model_name) if part.strip()]

            for part in parts:
                # Check for parentheses within each part
                base_model_match = re.match(r'^(.*)\((.*)\)$', part.strip())

                if base_model_match:
                    # Split the base model and the variants within parentheses
                    base_model = base_model_match.group(1).strip()
                    variants = base_model_match.group(2).strip().split(',')

                    for variant in variants:
                        variant = variant.strip()
                        data.append([brand_name, msisdn, imei_tac, f"{base_model} {variant}", os_name, os_vendor])
                else:
                    # No parentheses, just add the part
                    data.append([brand_name, msisdn, imei_tac, part.strip(), os_name, os_vendor])

    # Insert data into the normalized table
    insert_df = session.create_dataframe(data, schema=["BRAND_NAME", "MSISDN", "IMEI_TAC", "MODEL_NAME", "OS_NAME", "OS_VENDOR"])
    insert_df.write.mode("append").save_as_table("project_sec3_db.public.NORMALIZED_DEVICE_TABLE_1NF")

    return "Data transformation and insertion completed successfully."
$$;

CALL SPLIT_MODEL_NAME();