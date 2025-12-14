SELECT
    f.store_id,
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
    f.store_date,
    CURRENT_TIMESTAMP() AS insert_date,
    CURRENT_TIMESTAMP() AS update_date
FROM {{ ref("stg_department_raw")}} d
JOIN {{ ref("stg_fact_raw")}} f 
ON d.store_date = f.store_date AND d.store_id = f.store_id