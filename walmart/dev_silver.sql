USE ROLE accountadmin;
USE WAREHOUSE compute_wh;

SELECT * FROM walmart_db.bronze.stores_raw; -- stores (store, type, size)
SELECT * FROM walmart_db.bronze.department_raw; -- department (store, dept, date, weekly_sales, isholiday)
SELECT * FROM walmart_db.bronze.fact_raw; -- fact (store, date, temperature, fuel_price, markdown1...5, cpi, unemployment, isholiday)

CREATE OR REPLACE SCHEMA silver;
USE SCHEMA walmart_db.silver;


-- fact
-- department (store, dept, date, weekly_sales, isholiday)
-- fact (store, date, temperature, fuel_price, markdown1...5, cpi, unemployment, isholiday)
-- dept ()
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




SELECT store, date, temperature, cpi, isholiday
FROM bronze.fact_raw;





CREATE OR REPLACE TEMP TABLE silver.date_dim AS
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
SELECT * FROM silver.date_dim;



-- store_dim
CREATE OR REPLACE TEMP TABLE store_dim AS
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
