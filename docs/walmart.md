# Walmart Retail Analytics Pipeline

## Business goal
Build clean, analytics-ready datasets to study weekly store department sales and how they relate to store attributes and external drivers (fuel price, temperature, unemployment, CPI, markdowns, holidays).

Inputs:
- store attributes (type, size)
- weekly sales by store + department
- weekly external factors + markdowns by store

Outputs: 
- `dim_store` (SCD1) - grain: (store_id, dept_id)
- `dim_date` (SCD1) - grain: (store_date) (week-ending Friday)
- `fct_sales_enriched` - grain: (store_id, dept_id, store_date)
- `sales_enriched_snapshot` (SCD2) - history of enriched sales
- `weekly_sales` - denormalized view used by Python

## Architecture
This pipeline ingests Walmart retail datasets from S3, builds fact and dimensions tables in Snowflake using dbt, captures historical changes, and produces visualizations for BI reporting in Python.

![](arch.png)

#### Storage layout
- S3 bucket contains CSV data for each dataset
- Snowflake defines the following schemas and objects:
  - `bronze`: external stage (S3) + raw tables + staging views
  - `silver`: conformed dimensions (SCD1) + enriched fact
  - `snapshots`: SCD2 history table (dbt snapshot)
  - `gold`: denormalized analytics view for consumption

#### Pipeline flow (dbt)

1. Ingest raw CSVs from S3
    - `load_csv` macro runs `COPY INTO` command into raw tables: `stores_raw`, `department_raw`, `fact_raw` (bronze)
    - Triggered via pre-hook on each staging model
2. Build staging models (bronze)
    - `stg_stores` (grain: store)
    - `stg_dept_sales` (grain: store-department-week)
    - `stg_signals` (grain: store-week)
3. Build silver models 
    - `dim_store` (SCD1, incremental merge) - store + department with latest store attributes
    - `dim_date` (SCD1, incremental merge) - weekly date spine + flags (e.g. isholiday)
    - `fct_sales_enriched` - joins weekly sales to signals at store-department-week grain
4. Build snapshot
    - `sales_enriched_snapshot` captures historical changes for selected measures
5. Build gold model
    - `weekly_sales` joins fact and dimensions to denormalized analytics view
6. Query and visualize (Python notebook)
    - Python-snowflake connector is used to query final tables in Snowflake
    - Matplotlib & seaborn used to create visualizations for BI reporting
  


## Running pipeline
1) Provision Snowflake objects: `snowflake/walmart.sql`  
2) Run dbt:
```bash
dbt build --select models/walmart/bronze
dbt build --select models/walmart/silver
dbt snapshot
dbt build --select models/walmart/gold
```

## DBT Lineage Graph

![](dbt_lineage.png)



## Visualizations
Notebook: [`walmart_analysis`](../visualizations/walmart_analysis.ipynb).

![](../visualizations/sales_by_store_holiday.png)
![](../visualizations/weekly_sales_by_store_type.png)
![](../visualizations/weekly_sales_by_year.png)
![](../visualizations/yearly_markdown_sales.png)


## Project files
```text
├── snowflake
│   └── walmart
│       └── walmart.sql
│
├── visualizations
│       └── walmart_analysis.ipynb
│
└── dbt
    ├── macros
    │   └── load_csv.sql
    │
    ├── models
    │   └── walmart
    │       ├── bronze
    │       │   ├── _bronze.yml
    │       │   ├── stg_dept_sales.sql
    │       │   ├── stg_signals.sql
    │       │   └── stg_stores.sql
    │       ├── silver
    │       │   ├── _silver.yml
    │       │   ├── dim_store.sql
    │       │   ├── dim_date.sql
    │       │   └── fct_sales_enriched.sql
    │       └── gold
    │           └── weekly_sales.sql
    │
    ├── snapshots
    │   └── sales_enriched_snapshot.sql
    │
    ├── packages.yml        # shared
    └── dbt_project.yml     # shared (multi-project)
```

- `snowflake/walmart.sql` creates database, schemas, external stage, file format, and raw tables in Snowflake
- `dbt/macros/load_csv.sql` defines pre-hook ingestion macro
- `dbt/models/walmart/*` defines dbt-managed models
- `dbt/snapshots/sales_enriched_snapshot.sql` defines the SCD2 snapshot
