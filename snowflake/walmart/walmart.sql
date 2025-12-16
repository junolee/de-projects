USE ROLE accountadmin;
USE WAREHOUSE compute_wh;

CREATE OR REPLACE DATABASE walmart_db;
USE DATABASE walmart_db;

CREATE OR REPLACE SCHEMA bronze;
CREATE OR REPLACE SCHEMA silver;
CREATE OR REPLACE SCHEMA snapshots;
CREATE OR REPLACE SCHEMA gold;


-- Create file format and external stage

USE SCHEMA walmart_db.bronze;

CREATE OR REPLACE FILE FORMAT walmart_db.bronze.csv_format
TYPE = CSV
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
PARSE_HEADER=TRUE
ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE
NULL_IF = ('NULL', 'null')
EMPTY_FIELD_AS_NULL = true;

CREATE OR REPLACE STAGE walmart_db.bronze.walmart_s3_stage
STORAGE_INTEGRATION = s3_int
URL = 's3://jl-walmart/data/'
FILE_FORMAT = walmart_db.bronze.csv_format;
ls @walmart_db.bronze.walmart_s3_stage;


-- Define source tables

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
    UPDATE_DATE TIMESTAMP,
    LOADED_AT TIMESTAMP
);

CREATE OR REPLACE TABLE walmart_db.silver.walmart_store_dim (
    STORE_ID    INT,
    DEPT_ID     INT,
    STORE_TYPE  VARCHAR,
    STORE_SIZE  INT,
    INSERT_DATE TIMESTAMP,
    UPDATE_DATE TIMESTAMP,
    LOADED_AT TIMESTAMP
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
    VRSN_END_DATE       TIMESTAMP,
    LOADED_AT TIMESTAMP
);

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
