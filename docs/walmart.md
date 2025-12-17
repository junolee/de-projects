# Walmart Retail Analytics Pipeline




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
    │       │   ├── stg_stores_raw.sql
    │       │   ├── stg_department_raw.sql
    │       │   └── stg_fact_raw.sql
    │       ├── silver
    │       │   ├── walmart_store_dim.sql
    │       │   ├── walmart_date_dim.sql
    │       │   └── walmart_fact_table.sql
    │       └── gold
    │           └── weekly_sales.sql
    │
    ├── snapshots
    │   └── walmart_fact_snapshot.sql
    │
    ├── packages.yml        # shared
    └── dbt_project.yml     # shared (multi-project)
```

## DBT Models

![](dbt_lineage.png)

## Snowflake Setup
Create database and define source tables in Snowflake SQL file.


## DBT Job
Execute DBT commands in `dbt/` directory to build pipeline.
```
dbt build --select "models/walmart/bronze"
dbt build --select "models/walmart/silver"
dbt snapshot
dbt build --select "models/walmart/gold"
```

## Visualizations
Analyze and build visualizations using Python in [`walmart_analysis`](../visualizations/walmart_analysis.ipynb).