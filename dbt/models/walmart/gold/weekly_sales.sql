WITH fact AS (
    SELECT * FROM {{ ref("walmart_fact_snapshot") }}
    WHERE vrsn_end_date is NULL
), stores AS (
    SELECT 
        store_id,
        store_type,
        store_size
    FROM {{ ref("walmart_store_dim") }}
    GROUP BY (store_id, store_type, store_size)
), dates AS (
    SELECT * FROM {{ ref("walmart_date_dim") }}
)
SELECT
    f.store_id,
    f.store_date,
    d.isholiday,
    f.dept_id,
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
    f.markdown5,
FROM fact f
JOIN dates d ON f.store_date = d.store_date
JOIN stores s ON f.store_id = s.store_id