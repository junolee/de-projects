{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='date_id'
    )
}}

WITH dates AS ( {{ 
    dbt_utils.date_spine(
        datepart="week",
        start_date="cast('2010-02-05' as date)",
        end_date="cast('2013-07-26' as date)") }}
), stg_fact AS (
    SELECT
        store_date,
        isholiday
    FROM {{ ref("stg_fact_raw") }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(update_date) FROM {{ this }})
    {% endif %}
    GROUP BY (store_date, isholiday)
), existing AS (
    SELECT date_id, store_date, isholiday, insert_date, update_date
    FROM {{ this }}
)
SELECT
    TO_VARCHAR(d.date_week, 'yyyyMMdd')::INT AS date_id,
    d.date_week AS store_date,
    f.isholiday,
    COALESCE( e.insert_date, CURRENT_TIMESTAMP() ) AS insert_date,
    CURRENT_TIMESTAMP() AS update_date
FROM dates AS d
LEFT OUTER JOIN stg_fact AS f ON d.date_week = f.store_date
LEFT JOIN existing e ON d.date_week = e.store_date

