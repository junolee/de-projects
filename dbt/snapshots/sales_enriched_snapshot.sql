/*
  sales_enriched_snapshot (SCD2)

  Tracks history for fct_sales_enriched using dbt snapshot (strategy: check).

  Business key: (store_id, dept_id, store_date)
  Validity: vrsn_start_date / vrsn_end_date
  
  New version: any check_cols change
  Active record: vrsn_end_date is NULL
*/

{% snapshot sales_enriched_snapshot %}
select * from {{ ref('fct_sales_enriched') }}
{% endsnapshot %}