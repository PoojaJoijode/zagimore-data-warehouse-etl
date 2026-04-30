# Zagimore Data Warehouse ETL SQL Project

This repository contains SQL scripts for a **Zagimore Data Warehouse and ETL project**. The project demonstrates how transactional source data can be moved into a staging layer, transformed into a dimensional warehouse model, refreshed incrementally, and summarized through aggregate tables for reporting.

## Project Objective

The purpose of this project is to design and implement a data warehousing workflow for the Zagimore operational database. The SQL scripts cover schema creation, dimension loading, revenue fact loading, refresh procedures, slowly changing dimension logic, and aggregate table creation.

## Repository Structure

```text
zagimore-data-warehouse-etl-github-ready/
├── README.md
├── .gitignore
├── docs/
│   ├── architecture.md
│   ├── review_notes.md
│   └── run_order.md
└── sql/
    ├── 01_data_staging_schema.sql
    ├── 02_data_warehouse_schema.sql
    ├── 03_calendar_and_base_dimension_loads.sql
    ├── 04_initial_etl_load.sql
    ├── 05_revenue_fact_initial_load.sql
    ├── 06_revenue_fact_refresh_procedures.sql
    ├── 07_product_dimension_refresh_scd_type2.sql
    ├── 08_category_and_daily_store_aggregates.sql
    └── 09_region_aggregate.sql
```

## File Execution Order

| Step | File | Purpose |
|---:|---|---|
| 1 | `01_data_staging_schema.sql` | Creates staging dimension and fact tables. |
| 2 | `02_data_warehouse_schema.sql` | Creates the warehouse schema tables. |
| 3 | `03_calendar_and_base_dimension_loads.sql` | Loads calendar/base dimension logic. |
| 4 | `04_initial_etl_load.sql` | Performs initial ETL/ELT loading. |
| 5 | `05_revenue_fact_initial_load.sql` | Builds initial revenue fact extraction/loading logic. |
| 6 | `06_revenue_fact_refresh_procedures.sql` | Refreshes fact table data. |
| 7 | `07_product_dimension_refresh_scd_type2.sql` | Handles product dimension refresh and SCD Type 2 changes. |
| 8 | `08_category_and_daily_store_aggregates.sql` | Creates category and daily store aggregate tables. |
| 9 | `09_region_aggregate.sql` | Creates region dimension and region aggregate logic. |

## Data Warehouse Concepts Covered

- Data staging schema design
- Data warehouse schema design
- Dimension tables
- Revenue fact table
- ETL/ELT loading logic
- Fact table refresh process
- Slowly Changing Dimension Type 2 logic
- Aggregate tables for reporting
- Category, store, calendar, product, customer, and region analysis

## Suggested GitHub Description

**SQL-based data warehousing and ETL project using the Zagimore transactional database, including staging schema, warehouse schema, fact/dimension loading, SCD Type 2 refresh logic, and aggregate reporting tables.**



## Notes

The SQL files are organized for portfolio presentation. Some scripts may still require environment-specific schema names, table names, or syntax adjustments before running on a different MySQL setup.
