/*
  weekly_sales

  Denormalized analytics view of weekly dept sales + weekly signals + store attributes
  Grain: store_id, dept_id, store_date (week)
  Inputs: fct_sales_enriched, dim_store, dim_date
*/

WITH fact AS (
    SELECT
        store_id,
        store_date,
        dept_id,
        store_weekly_sales, 
        fuel_price,
        store_temperature,
        unemployment,
        cpi,
        markdown1,
        markdown2,
        markdown3,
        markdown4,
        markdown5
    FROM {{ ref("fct_sales_enriched") }}
), 

stores AS (
    SELECT
        store_id,
        dept_id,
        store_type,
        store_size
    FROM {{ ref("dim_store") }}
), 

dates AS (
    SELECT
        date_id,
        store_date,
        isholiday
    FROM {{ ref("dim_date") }}
)

SELECT
    f.store_id,
    f.dept_id, 
    f.store_date,       
    d.isholiday,
    s.store_type,
    s.store_size,
    f.store_weekly_sales, 
    f.fuel_price,
    f.store_temperature,
    f.unemployment,
    f.cpi,
    f.markdown1,
    f.markdown2,
    f.markdown3,
    f.markdown4,
    f.markdown5
FROM fact f
JOIN stores s ON f.store_id = s.store_id AND f.dept_id = s.dept_id
JOIN dates d ON f.store_date = d.store_date