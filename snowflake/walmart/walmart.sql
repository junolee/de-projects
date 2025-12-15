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
USE DATABASE walmart_db;

CREATE OR REPLACE SCHEMA bronze;
CREATE OR REPLACE SCHEMA silver;
CREATE OR REPLACE SCHEMA snapshots;
CREATE OR REPLACE SCHEMA gold;


-- Create file format and external stage

USE SCHEMA walmart_db.bronze;

CREATE OR REPLACE FILE FORMAT bronze.csv_format
TYPE = CSV
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
NULL_IF = ('NULL', 'null')
EMPTY_FIELD_AS_NULL = true;

CREATE OR REPLACE STAGE bronze.walmart_s3_stage
STORAGE_INTEGRATION = s3_int
URL = 's3://jl-walmart/data/'
FILE_FORMAT = csv_format;
ls @walmart_s3_stage;


-- Define source tables

USE SCHEMA walmart_db.bronze;

CREATE OR REPLACE TABLE walmart_db.bronze.stores_raw (
    STORE STRING,
    TYPE STRING,
    SIZE STRING,
    LOADED_AT TIMESTAMP
);

CREATE OR REPLACE TABLE walmart_db.bronze.department_raw (
    STORE STRING,
    DEPT STRING,
    DATE STRING,
    WEEKLY_SALES STRING,
    ISHOLIDAY STRING,
    LOADED_AT TIMESTAMP
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
    ISHOLIDAY STRING,
    LOADED_AT TIMESTAMP
);

-- Define target tables

USE SCHEMA walmart_db.silver;

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
    STORE_SIZE  INT,
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




-- Load source tables
USE SCHEMA walmart_db.bronze;

COPY INTO walmart_db.bronze.stores_raw
FROM (
    SELECT
        $1 AS store,
        $2 AS type,
        $3 AS size,
        CURRENT_TIMESTAMP() AS loaded_at
FROM @walmart_s3_stage/stores.csv
) FILE_FORMAT = csv_format;

COPY INTO walmart_db.bronze.department_raw
FROM (
    SELECT
        $1 AS store,
        $2 AS dept,
        $3 AS date,
        $4 AS weekly_sales,
        $5 AS isholiday,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM @walmart_s3_stage/department.csv
) FILE_FORMAT = csv_format;

COPY INTO walmart_db.bronze.fact_raw
FROM (
    SELECT
        $1 AS store,
        $2 AS date,
        $3 AS temperature,
        $4 AS fuel_price,
        $5 AS markdown1,
        $6 AS markdown2,
        $7 AS markdown3,
        $8 AS markdown4,
        $9 AS markdown5,
        $10 AS cpi,
        $11 AS unemployment,
        $12 AS isholiday,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM @walmart_s3_stage/fact.csv
) FILE_FORMAT = csv_format;



-- Queries

-- after loading
SELECT * FROM walmart_db.bronze.stores_raw;
SELECT * FROM walmart_db.bronze.department_raw;
SELECT * FROM walmart_db.bronze.fact_raw;

-- after building dbt staging models
SELECT * FROM walmart_db.bronze.stg_stores_raw;
SELECT * FROM walmart_db.bronze.stg_department_raw;
SELECT * FROM walmart_db.bronze.stg_fact_raw;

-- after building dbt silver models
SELECT * FROM silver.walmart_store_dim;
SELECT * FROM silver.walmart_date_dim;
SELECT * FROM silver.walmart_fact_table;
SELECT * FROM snapshots.walmart_fact_snapshot;
SELECT * FROM gold.weekly_sales;

