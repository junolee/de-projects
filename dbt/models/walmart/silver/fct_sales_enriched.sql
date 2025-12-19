/*
  fct_sales_enriched (current state)

  Weekly department sales joined with weekly signals (PK: store_id, dept_id, store_date)

  Inputs: stg_dept_sales, stg_signals

  Logic:
  - Deduplicate each input to latest record per key by loaded_at
  - Left join sales(store-dept-week) to signals (store-week)on date and store
  
  History:
  - Changes captured via snapshot: sales_enriched_snapshot
 
*/

WITH signals AS (
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
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY store_id, store_date 
        ORDER BY loaded_at DESC ) = 1
), 

dept_sales AS (
    SELECT
        store_id,
        store_date,
        dept_id,
        store_weekly_sales,
        loaded_at
    FROM {{ ref("stg_dept_sales") }}
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY store_id, store_date, dept_id 
        ORDER BY loaded_at DESC ) = 1
)

SELECT
    d.store_id,
    d.store_date,
    d.dept_id,
    d.store_weekly_sales, 
    s.fuel_price,
    s.store_temperature,
    s.unemployment,
    s.cpi,
    s.markdown1,
    s.markdown2,
    s.markdown3,
    s.markdown4,
    s.markdown5,
    COALESCE(GREATEST(d.loaded_at, s.loaded_at), d.loaded_at) AS loaded_at
FROM dept_sales d
LEFT JOIN signals s
ON d.store_date = s.store_date AND d.store_id = s.store_id