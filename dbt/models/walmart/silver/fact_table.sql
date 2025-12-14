WITH stg_fact AS (
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
        markdown5
    FROM {{ ref("stg_fact_raw") }}
), stg_dept AS (
    SELECT
        store_id,
        store_date,
        dept_id,
        store_weekly_sales
    FROM {{ ref("stg_department_raw") }}
)
SELECT
    f.store_id,
    f.store_date, -- remove?
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
    CURRENT_TIMESTAMP() AS insert_date,
    CURRENT_TIMESTAMP() AS update_date
    FROM stg_dept d
    JOIN stg_fact f 
    ON d.store_date = f.store_date AND d.store_id = f.store_id