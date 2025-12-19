WITH deduped_store_signals AS (
    SELECT
        store_id,
        store_date,
        fuel_price,
        store_temperature,
        unemployment,
        cpi,
        markdown1,
        markdown2,
        markdown3,
        markdown4,
        markdown5,
        loaded_at
    FROM {{ ref("stg_signals") }}
    -- Deduplicate by latest loaded_at for freshest record per store-date
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY store_id, store_date 
        ORDER BY loaded_at DESC ) = 1
), 

deduped_dept_sales AS (
    SELECT
        store_id,
        store_date,
        dept_id,
        store_weekly_sales,
        loaded_at
    FROM {{ ref("stg_dept_sales") }}
    -- Deduplicate by latest loaded_at for freshest record per store-dept-date
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY store_id, store_date, dept_id 
        ORDER BY loaded_at DESC ) = 1
)

SELECT
    f.store_id,
    f.store_date,
    d.dept_id,
    d.store_weekly_sales, 
    f.fuel_price,
    f.store_temperature,
    f.unemployment,
    f.cpi,
    f.markdown1,
    f.markdown2,
    f.markdown3,
    f.markdown4,
    f.markdown5,
    f.loaded_at
FROM deduped_dept_sales d       -- Weekly sales per department per store
JOIN deduped_store_signals f    -- Weekly signals per store
-- Join to add weekly store-level signals to all departments in that store for the week
ON d.store_date = f.store_date AND d.store_id = f.store_id