{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['store_id', 'dept_id']
    )
}}

WITH stg_dept AS (
    SELECT 
        store_id,
        dept_id
    FROM {{ ref("stg_department_raw") }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(update_date) FROM {{ this }})
    {% endif %}
    GROUP BY (store_id, dept_id)
),
stg_stores AS (
    SELECT
        store_id,
        store_type
    FROM {{ ref("stg_stores_raw") }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(update_date) FROM {{ this }})
    {% endif %}
),
existing AS (
    SELECT store_id, dept_id, insert_date
    FROM {{ this }}
)
SELECT 
    d.store_id,
    d.dept_id,
    s.store_type,
    COALESCE( e.insert_date, CURRENT_TIMESTAMP() ) AS insert_date,
    CURRENT_TIMESTAMP() AS update_date
FROM stg_dept d
JOIN stg_stores s ON d.store_id = s.store_id
LEFT JOIN existing e ON e.store_id = d.store_id AND e.dept_id = d.dept_id
