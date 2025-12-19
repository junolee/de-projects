/*
  weekly_sales

  Denormalized analytics view of weekly dept sales + weekly signals + store attributes
  Grain: store_id, dept_id, store_date (week)
  Inputs: sales_enriched_snapshot, dim_store, dim_date

  Notes:
  - Filters snapshot to active records (vrsn_end_date IS NULL)
*/

WITH fact_active AS (
    SELECT * FROM {{ ref("sales_enriched_snapshot") }}
    WHERE vrsn_end_date IS NULL
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
    SELECT * FROM {{ ref("dim_date") }}
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
FROM fact_active f
JOIN dates d ON f.store_date = d.store_date
JOIN stores s ON f.store_id = s.store_id AND f.dept_id = s.dept_id