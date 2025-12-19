/*
  stg_dept_sales

  Staging model for weekly department sales
  Grain: 1 row per store-dept-week
  Source: walmart.dept_sales_raw

  Notes:
  - Renames + type casts
  - loaded_at injected via Snowflake COPY INTO INCLUDE_METADATA (scan time)
*/

WITH source AS (
    SELECT * from {{ source('walmart', 'dept_sales_raw')}}
),
renamed as (
    SELECT
        store::INT AS store_id,
        dept::INT AS dept_id,
        date::DATE AS store_date,
        CAST(weekly_sales * 100 AS INT) / 100.0 AS store_weekly_sales,
        isholiday::BOOLEAN AS isholiday,
        loaded_at
    FROM source
)

SELECT * FROM renamed