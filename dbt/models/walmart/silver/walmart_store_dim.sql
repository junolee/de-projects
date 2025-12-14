WITH stg_dept AS (
    SELECT 
        store_id,
        dept_id
    FROM {{ ref("stg_department_raw") }}
    GROUP BY (store_id, dept_id)
),
stg_stores AS (
    SELECT
        store_id,
        store_type
    FROM {{ ref("stg_stores_raw") }}
)
SELECT 
    d.store_id,
    d.dept_id,
    s.store_type,
    CURRENT_TIMESTAMP() AS insert_date,
    CURRENT_TIMESTAMP() AS update_date
FROM stg_dept d
JOIN stg_stores s
ON d.store_id = s.store_id