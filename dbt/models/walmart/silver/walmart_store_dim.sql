{{ config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['store_id', 'dept_id']) 
}}

WITH watermark AS (
  SELECT COALESCE(MAX(loaded_at), '1900-01-01'::timestamp) AS wm
  FROM {{ this }}
),

changed_stores AS (
  {% if is_incremental() %}
    SELECT DISTINCT store_id 
    FROM {{ ref("stg_stores_raw") }} 
    WHERE loaded_at > (SELECT wm FROM watermark)

    UNION

    SELECT DISTINCT store_id 
    FROM {{ ref("stg_department_raw") }} 
    WHERE loaded_at > (SELECT wm FROM watermark)
  {% else %}
    SELECT DISTINCT store_id FROM {{ ref("stg_stores_raw") }} 
    UNION
    SELECT DISTINCT store_id FROM {{ ref("stg_department_raw") }} 
  {% endif %}
), 

store_dept_keys AS (
    SELECT 
        store_id, 
        dept_id, 
        MAX(loaded_at) AS loaded_at 
    FROM {{ ref("stg_department_raw") }}
    {% if is_incremental() %}
    WHERE store_id IN (SELECT store_id FROM changed_stores)
    {% endif %}
    GROUP BY 1, 2 
), 

stores_latest AS (
    SELECT 
        store_id,
        store_type,
        store_size,
        loaded_at
    FROM {{ ref('stg_stores_raw') }}
    {% if is_incremental() %}
    WHERE store_id IN (SELECT store_id FROM changed_stores)
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY loaded_at DESC) = 1

), 

existing AS (
    SELECT store_id, dept_id, insert_date
    FROM {{ this }}
)

SELECT 
    d.store_id,
    d.dept_id,
    s.store_type,
    s.store_size,
    COALESCE( e.insert_date, CURRENT_TIMESTAMP() ) AS insert_date,
    CURRENT_TIMESTAMP() AS update_date,
    GREATEST(d.loaded_at, s.loaded_at) AS loaded_at
FROM store_dept_keys d
JOIN stores_latest s
    ON d.store_id = s.store_id
LEFT JOIN existing e
    ON d.store_id = e.store_id AND d.dept_id = e.dept_id