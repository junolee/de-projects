{% snapshot sales_enriched_snapshot %}
select * from {{ ref('fct_sales_enriched') }}
{% endsnapshot %}
