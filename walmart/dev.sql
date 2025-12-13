USE ROLE accountadmin;
USE WAREHOUSE compute_wh;

SELECT * FROM walmart_db.bronze.stores_raw; -- stores (store, type, size)
SELECT * FROM walmart_db.bronze.department_raw; -- department (store, dept, date, weekly_sales, isholiday)
SELECT * FROM walmart_db.bronze.fact_raw; -- fact (store, date, temperature, fuel_price, markdown1...5, cpi, unemployment, isholiday)

USE SCHEMA walmart_db.silver;

-- query for walmart_date_dim
SELECT 
    ROW_NUMBER() OVER (ORDER BY date) AS date_id,
    date AS store_date,
    isholiday
FROM (
    SELECT 
        TO_DATE(date) AS date,
        isholiday::BOOLEAN AS isholiday
    FROM bronze.fact_raw
    GROUP BY date, isholiday
    ORDER BY date
);

-- query for walmart_store_dim
WITH depts AS (
    SELECT store, dept
    FROM bronze.department_raw
    GROUP BY (store, dept)
)
SELECT 
    d.store AS store_id,
    d.dept AS dept_id,
    s.type AS store_type,
    CURRENT_TIMESTAMP() AS insert_date,
    CURRENT_TIMESTAMP() AS update_date
FROM depts d
JOIN bronze.stores_raw s
ON d.store = s.store;

-- query for walmart_fact_table
SELECT
    f.store AS store_id,
    d.dept AS dept_id, 
    d.weekly_sales, 
    f.temperature AS store_temperature,
    f.fuel_price,
    f.markdown1,
    f.markdown2,
    f.markdown3,
    f.markdown4,
    f.markdown5,
    f.cpi,
    f.unemployment,
    f.isholiday,
    CURRENT_TIMESTAMP() AS INSERT_DTS,
    CURRENT_TIMESTAMP() AS UPDATE_DTS,
    d.date AS VRSN_START_DATE,
    NULL AS VRSN_END_DATE
FROM bronze.department_raw d
JOIN bronze.fact_raw f 
ON d.date = f.date AND d.store = f.store;