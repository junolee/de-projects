/*
  dim_date - incremental model (SCD1)

  Weekly date dimension (Friday) with continuous coverage via dbt_utils.date_spine (PK: date_id, natural key: store_date)

  Inputs: stg_signals (observed weeks + isholiday)

  Incremental strategy:
    - Watermark = MAX(loaded_at) in {{ this }}
    - Recompute only store_date with new rows in stg_signals since watermark

  Notes:
    - stg_signals de-duped to latest per store_date by loaded_at
    - insert_date preserved on updates
    - date key generated from date
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

changed_dates AS (
  {% if is_incremental() %}
    SELECT DISTINCT store_date 
    FROM {{ ref("stg_signals") }} 
    WHERE loaded_at > (SELECT wm FROM watermark)
  {% else %}
    SELECT DISTINCT store_date FROM {{ ref("stg_signals") }}
  {% endif %}
), 

fact_dates AS (
    SELECT
        store_date,
        isholiday,
        loaded_at
    FROM {{ ref("stg_signals") }}
    {% if is_incremental() %}
    WHERE store_date in (SELECT store_date FROM changed_dates)
    {% endif %}
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY store_date 
        ORDER BY loaded_at DESC ) = 1
),

date_spine AS (
    SELECT *
    FROM ( {{ dbt_utils.date_spine(
                datepart="week",
                start_date="cast('2010-02-05' as date)",
                end_date="cast('2013-07-26' as date)") }})

),

existing AS (
  {% if is_incremental() %}
    SELECT store_date, isholiday, insert_date
    FROM {{ this }}
  {% else %}
    SELECT
      CAST(NULL AS DATE) AS store_date,
      CAST(NULL AS BOOLEAN) AS isholiday,
      CAST(NULL AS TIMESTAMP) AS insert_date
    WHERE 1=0
  {% endif %}
)

SELECT
    TO_VARCHAR(d.date_week, 'yyyyMMdd')::INT AS date_id,
    d.date_week AS store_date,
    COALESCE( f.isholiday, e.isholiday) AS isholiday,
    COALESCE( e.insert_date, CURRENT_TIMESTAMP() ) AS insert_date,
    CURRENT_TIMESTAMP() AS update_date,
    COALESCE(f.loaded_at, CURRENT_TIMESTAMP()) AS loaded_at
FROM date_spine d
LEFT JOIN fact_dates f
    ON d.date_week = f.store_date
LEFT JOIN existing e
    ON d.date_week = e.store_date
{% if is_incremental() %}
    WHERE d.date_week in (SELECT store_date FROM changed_dates)
{% endif %}