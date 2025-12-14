{% snapshot walmart_fact_snapshot %}
{{
    config(
      target_database='walmart_db',
      target_schema='snapshots',
      unique_key=['store_id', 'dept_id', 'store_date'],
      strategy='timestamp',
      updated_at='loaded_at',
      snapshot_meta_column_names={
        'dbt_updated_at': 'update_date',
        'dbt_valid_from': 'vrsn_start_date',
        'dbt_valid_to': 'vrsn_end_date',
        'dbt_scd_id': 'scd_id'
      }
    )
}}
select * from {{ ref('walmart_fact_table') }}
{% endsnapshot %}
