WITH source AS (
    SELECT * from {{ source('walmart', 'department_raw')}}
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