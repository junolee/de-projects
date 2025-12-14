USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE SCHEMA walmart_db.silver;

-- MERGE

-- date_dim

MERGE INTO silver.walmart_date_dim tgt
USING silver.date_dim src
    ON tgt.date_id = src.date_id
WHEN MATCHED THEN
    UPDATE SET
        tgt.store_date = src.store_date,
        tgt.isholiday = src.isholiday,
        tgt.update_date = src.loaded_at
WHEN NOT MATCHED THEN 
    INSERT ( 
        date_id, 
        store_date, 
        isholiday, 
        insert_date, 
        update_date 
    )
    VALUES ( 
        src.date_id, 
        src.store_date, 
        src.isholiday, 
        src.loaded_at, 
        src.loaded_at 
    );


-- store_dim

MERGE INTO silver.walmart_store_dim tgt
USING silver.store_dim src
    ON tgt.store_id = src.store_id 
    AND tgt.dept_id = src.dept_id
WHEN MATCHED THEN
    UPDATE SET
        tgt.store_id = src.store_id,
        tgt.dept_id = src. dept_id,
        tgt.store_type = src.store_type,
        tgt.update_date = src.loaded_at
WHEN NOT MATCHED THEN
    INSERT ( 
        store_id, 
        dept_id, 
        store_type, 
        insert_date, 
        update_date 
    )
    VALUES ( 
        t.store_id, 
        t.dept_id, 
        t.store_type, 
        t.loaded_at, 
        t.loaded_at 
    );


DROP TABLE snapshots.walmart_fact_snapshot;
SELECT * FROM snapshots.walmart_fact_snapshot;

-- fact_table

MERGE INTO silver.walmart_fact_table tgt
USING silver.fact_table src
    ON tgt.store_id = src.store_id 
    AND tgt.dept_id = src.dept_id 
    AND tgt.store_date = src.store_date
    AND tgt.vrsn_end_date IS NULL
WHEN MATCHED THEN
    UPDATE SET
        tgt.vrsn_end_date = src.loaded_at -- effective_date; usually load_date
WHEN NOT MATCHED THEN 
    INSERT ( 
        store_id, dept_id, store_date, store_weekly_sales, fuel_price, store_temperature, 
        unemployment, cpi, markdown1, markdown2, markdown3, markdown4, markdown5, 
        insert_date, 
        update_date, 
        vrsn_start_date, 
        vrsn_end_date 
    )
    VALUES ( 
        t.store_id, t.dept_id, t.store_date, t.store_weekly_sales, t.fuel_price, t.store_temperature, 
        t.unemployment, t.cpi, t.markdown1, t.markdown2, t.markdown3, t.markdown4, t.markdown5, 
        t.loaded_at, 
        t.loaded_at, 
        t.loaded_at, 
        NULL 
    );

