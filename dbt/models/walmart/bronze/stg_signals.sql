/*
  stg_signals

  Staging model for weekly signals
  Grain: 1 row per store-week
  Source: walmart.signals_raw

  Notes:
  - Renames, type casts, 'NA' -> NULL
  - loaded_at injected via Snowflake COPY INTO INCLUDE_METADATA (scan time)
*/

WITH source AS (
    SELECT * from {{ source('walmart', 'signals_raw')}}
),
renamed as (
    SELECT
        store::INT AS store_id,
        date::DATE AS store_date,
        NULLIF(temperature, 'NA')::DECIMAL(38, 2) AS store_temperature,
        NULLIF(fuel_price, 'NA')::DECIMAL(38, 3) AS fuel_price,
        NULLIF(markdown1, 'NA')::DECIMAL(38, 2) AS markdown1,
        NULLIF(markdown2, 'NA')::DECIMAL(38, 2) AS markdown2,
        NULLIF(markdown3, 'NA')::DECIMAL(38, 2) AS markdown3,
        NULLIF(markdown4, 'NA')::DECIMAL(38, 2) AS markdown4,
        NULLIF(markdown5, 'NA')::DECIMAL(38, 2) AS markdown5,
        NULLIF(cpi, 'NA')::DECIMAL(38, 8) AS cpi,
        NULLIF(unemployment, 'NA')::DECIMAL(38, 3) AS unemployment,
        isholiday::BOOLEAN AS isholiday,
        loaded_at
    FROM source
)

SELECT * FROM renamed