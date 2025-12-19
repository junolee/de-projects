/*
  stg_stores

  Staging model for stores with attributes (type, size)
  Grain: 1 row per store
  Source: walmart.stores_raw

  Notes:
  - Renames + type casts
  - loaded_at injected via Snowflake COPY INTO INCLUDE_METADATA (scan time)
*/

WITH source AS (
    SELECT * from {{ source('walmart', 'stores_raw')}}
),
renamed as (
    SELECT
        store::INT AS store_id,
        type::VARCHAR AS store_type,
        size::INT AS store_size,
        loaded_at
    FROM source
)

SELECT * FROM renamed