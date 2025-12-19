/*
  dim_store - incremental model (SCD1)

  All stores and its departments with latest store attributes (PK: store_id, dept_id)

  Inputs: stg_stores, stg_dept_sales

  Incremental strategy:
    - Watermark = MAX(loaded_at) in {{ this }}
    - Recompute only store_ids with new rows in either input since watermark

  Notes:
    - stg_stores de-duped to latest per store_id by loaded_at
    - insert_date preserved on updates

*/

WITH watermark AS (
  SELECT
    {% if is_incremental() %}
      (SELECT COALESCE(MAX(loaded_at), '1900-01-01'::timestamp) FROM {{ this }})
    {% else %}
      '1900-01-01'::timestamp
    {% endif %}
    AS wm
),

changed_stores AS (
  {% if is_incremental() %}
    SELECT DISTINCT store_id 
    FROM {{ ref("stg_stores") }} 
    WHERE loaded_at > (SELECT wm FROM watermark)

    UNION

    SELECT DISTINCT store_id 
    FROM {{ ref("stg_dept_sales") }} 
    WHERE loaded_at > (SELECT wm FROM watermark)
  {% else %}
    SELECT DISTINCT store_id FROM {{ ref("stg_stores") }} 
    UNION
    SELECT DISTINCT store_id FROM {{ ref("stg_dept_sales") }} 
  {% endif %}
), 

stores AS (
    SELECT 
        store_id,
        store_type,
        store_size,
        loaded_at
    FROM {{ ref('stg_stores') }}
    {% if is_incremental() %}
    WHERE store_id IN (SELECT store_id FROM changed_stores)
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY store_id 
      ORDER BY loaded_at DESC ) = 1
),

store_dept_keys AS (
    SELECT 
        store_id, 
        dept_id, 
        MAX(loaded_at) AS loaded_at 
    FROM {{ ref("stg_dept_sales") }}
    {% if is_incremental() %}
    WHERE store_id IN (SELECT store_id FROM changed_stores)
    {% endif %}
    GROUP BY 1, 2 
), 

existing AS (
  {% if is_incremental() %}
    SELECT store_id, dept_id, insert_date
    FROM {{ this }}
  {% else %}
    SELECT
      CAST(NULL AS INT) AS store_id,
      CAST(NULL AS INT) AS dept_id,
      CAST(NULL AS TIMESTAMP) AS insert_date
    WHERE 1=0
  {% endif %}
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
JOIN stores s
    ON d.store_id = s.store_id
LEFT JOIN existing e
    ON d.store_id = e.store_id AND d.dept_id = e.dept_id