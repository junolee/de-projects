USE ROLE accountadmin;
USE WAREHOUSE compute_wh;

CREATE OR REPLACE STORAGE INTEGRATION s3_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    STORAGE_AWS_ROLE_ARN = $aws_role_arn
    STORAGE_AWS_EXTERNAL_ID = $aws_external_id
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = (
    's3://jl-realtimeproject-data-bucket/data/',
    's3://jl-de-glueproject-bucket/data/',
    's3://jl-scd1-data-bucket/data/',
    's3://jl-scd2-data-bucket/raw_data/',
    's3://jl-dea-airflow-data-bucket'
);

DESC INTEGRATION s3_int;

-- DROP STORAGE INTEGRATION IF EXISTS S3_INT;