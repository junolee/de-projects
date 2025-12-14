WITH source AS (
    SELECT * from {{ source('walmart', 'stores_raw')}}
),
renamed as (
    SELECT
        store::INT AS store_id,
        type::VARCHAR AS store_type,
        size::INT AS store_size,
        insert_date,
        update_date
    FROM source
)

SELECT * FROM renamed