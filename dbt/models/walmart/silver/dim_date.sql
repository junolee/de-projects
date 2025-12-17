{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='date_id'
    )
}}

WITH watermark AS (
  SELECT COALESCE(MAX(loaded_at), '1900-01-01'::timestamp) AS wm
  FROM {{ this }}
),

fact_dates_latest AS (
    SELECT
        store_date,
        isholiday,
        loaded_at
    FROM {{ ref("stg_fact_raw") }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT wm FROM watermark)
    {% endif %}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY store_date ORDER BY loaded_at DESC) = 1
),

date_spine AS (
    SELECT * 
    FROM ( {{ dbt_utils.date_spine(
                datepart="week",
                start_date="cast('2010-02-05' as date)",
                end_date="cast('2013-07-26' as date)") }})
    
),

existing AS (
    SELECT store_date, isholiday, insert_date
    FROM {{ this }}
)


SELECT
    TO_VARCHAR(d.date_week, 'yyyyMMdd')::INT AS date_id,   -- generate date key from date
    d.date_week AS store_date,
    COALESCE( f.isholiday, e.isholiday) AS isholiday,   -- use latest isholiday value if exists; if latest isholiday is null, use existing value
    COALESCE( e.insert_date, CURRENT_TIMESTAMP() ) AS insert_date,   -- preserve existing insert date
    CURRENT_TIMESTAMP() AS update_date,
    loaded_at    
FROM date_spine d
LEFT JOIN fact_dates_latest f
    ON d.date_week = f.store_date
LEFT JOIN existing e
    ON d.date_week = e.store_date

{% if is_incremental() %}
    WHERE f.store_date IS NOT NULL     -- any dates coming from new fact
{% endif %}