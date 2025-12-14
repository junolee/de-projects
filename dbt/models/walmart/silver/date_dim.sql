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
)
SELECT
    TO_VARCHAR(d.date_week, 'yyyyMMdd')::INT AS date_id,
    d.date_week AS store_date,
    f.isholiday,
    CURRENT_TIMESTAMP() AS insert_date,
    CURRENT_TIMESTAMP() AS update_date
FROM dates AS d
LEFT OUTER JOIN stg_fact AS f ON d.date_week = f.store_date

