USE ROLE accountadmin;
USE WAREHOUSE compute_wh;

CREATE OR REPLACE STORAGE INTEGRATION s3_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    STORAGE_AWS_ROLE_ARN = $aws_role_arn
    STORAGE_AWS_EXTERNAL_ID = $aws_external_id
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = (
    's3://jl-walmart/data/'
);

DESC INTEGRATION s3_int;

CREATE OR REPLACE DATABASE walmart_db;
CREATE OR REPLACE SCHEMA bronze;
CREATE OR REPLACE SCHEMA silver;
USE SCHEMA walmart_db.bronze;

CREATE OR REPLACE FILE FORMAT csv_format
TYPE = CSV
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
NULL_IF = ('NULL', 'null')
EMPTY_FIELD_AS_NULL = true;

CREATE OR REPlACE STAGE walmart_s3_stage
STORAGE_INTEGRATION = s3_int
URL = 's3://jl-walmart/data/'
FILE_FORMAT = csv_format;

ls @walmart_s3_stage;

USE SCHEMA walmart_db.bronze;

CREATE OR REPLACE TABLE walmart_db.bronze.stores_raw (
    STORE STRING,
    TYPE STRING,
    SIZE STRING
);

CREATE OR REPLACE TABLE walmart_db.bronze.department_raw (
    STORE STRING,
    DEPT STRING,
    DATE STRING,
    WEEKLY_SALES STRING,
    ISHOLIDAY STRING
);

CREATE OR REPLACE TABLE walmart_db.bronze.fact_raw (
    STORE STRING,
    DATE STRING,
    TEMPERATURE STRING,
    FUEL_PRICE STRING,
    MARKDOWN1 STRING,
    MARKDOWN2 STRING,
    MARKDOWN3 STRING,
    MARKDOWN4 STRING,
    MARKDOWN5 STRING,
    CPI STRING,
    UNEMPLOYMENT STRING,
    ISHOLIDAY STRING
);

ls @walmart_s3_stage;

COPY INTO walmart_db.bronze.stores_raw
FROM @walmart_s3_stage/stores.csv
FILE_FORMAT = csv_format;

COPY INTO walmart_db.bronze.department_raw
FROM @walmart_s3_stage/department.csv
FILE_FORMAT = csv_format;

COPY INTO walmart_db.bronze.fact_raw
FROM @walmart_s3_stage/fact.csv
FILE_FORMAT = csv_format;

SELECT * FROM walmart_db.bronze.stores_raw;
SELECT * FROM walmart_db.bronze.department_raw;
SELECT * FROM walmart_db.bronze.fact_raw;

CREATE OR REPLACE TABLE walmart_db.silver.walmart_date_dim (
    DATE_ID     INT,
    STORE_DATE  DATE,
    ISHOLIDAY   VARCHAR,
    INSERT_DATE TIMESTAMP,
    UPDATE_DATE TIMESTAMP
);

CREATE OR REPLACE TABLE walmart_db.silver.walmart_store_dim (
    STORE_ID    INT,
    DEPT_ID     INT,
    STORE_TYPE  VARCHAR,
    INSERT_DATE TIMESTAMP,
    UPDATE_DATE TIMESTAMP
);

CREATE OR REPLACE TABLE walmart_db.silver.walmart_fact_table (
    STORE_ID            INT,
    DEPT_ID             INT,
    STORE_WEEKLY_SALES  DECIMAL,
    FUEL_PRICE          DECIMAL,
    STORE_TEMPERATURE   DECIMAL,
    UNEMPLOYMENT        DECIMAL,
    CPI                 DECIMAL,
    MARKDOWN1           DECIMAL,
    MARKDOWN2           DECIMAL,
    MARKDOWN3           DECIMAL,
    MARKDOWN4           DECIMAL,
    MARKDOWN5           DECIMAL,
    INSERT_DATE         TIMESTAMP,
    UPDATE_DATE         TIMESTAMP,
    VRSN_START_DATE     TIMESTAMP,
    VRSN_END_DATE       TIMESTAMP
);
