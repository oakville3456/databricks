# Azure Databricks Retail Pipeline

Production-grade **Bronze → Silver → Gold** data pipeline built on Azure Databricks, covering ingestion, transformation, upserts, governance, orchestration, streaming, and dashboarding.

---

## Architecture

```
ADLS raw-landing (CSV files)
        ↓  Auto Loader (cloudFiles)
┌─────────────────────────────────────────────────────────┐
│  BRONZE   abfss://bronze@saretailsalesdev...            │
│  Delta table · append mode · checkpoint dedup           │
│  1,100 rows · 5 stores · 3 dates                        │
└─────────────────────────┬───────────────────────────────┘
                          ↓  Delta MERGE upsert
┌─────────────────────────────────────────────────────────┐
│  SILVER   abfss://silver@saretailsalesdev...            │
│  Delta table · cleaned · deduplicated · typed           │
│  541 rows · try_to_date · revenue = qty × price         │
└─────────────────────────┬───────────────────────────────┘
                          ↓  groupBy store + date
┌─────────────────────────────────────────────────────────┐
│  GOLD     abfss://gold@saretailsalesdev...              │
│  Delta table · aggregated by store_id + order_date      │
│  15 rows · total_revenue · order_count · avg_value      │
└─────────────────────────────────────────────────────────┘
```

---

## Notebooks

| # | Notebook | Purpose | Priority |
|---|---|---|---|
| 01 | `01_bronze_ingest_Auto_load_4` | Auto Loader ingestion from ADLS raw-landing → Bronze Delta | 1 |
| 02 | `02_silver_transform_0604` | Bronze → Silver cleaning, typing, deduplication | 1 |
| 03 | `03_gold_aggregate_0604` | Silver → Gold aggregation by store + date | 1 |
| 04 | `04_silver_upsert` | Delta MERGE upsert — insert new, update changed, no duplicates | 3 |
| 05 | `05_time_travel` | Delta time travel, RESTORE TABLE, VACUUM | 4 |
| 06 | `06_unity_catalog` | Unity Catalog governance, column comments, cross-layer SQL | 5 |
| 07 | `07_workflows` | Pipeline health check (Bronze + Silver + Gold row counts) | 6 |
| 08 | `08_optimize` | OPTIMIZE + Z-ORDER + ANALYZE TABLE performance tuning | 7 |
| 09 | `09_streaming` | Structured Streaming — readStream, writeStream, outputModes | 8 |
| 10 | `10_dlt_pipeline` | Delta Live Tables — @dlt.table, @dlt.expect, auto-lineage | 9 |
| 11 | `11_sql_dashboard` | SQL queries powering the Retail Sales Dashboard | 10 |

---

## Pipelines

Three implementations of the same Bronze → Silver → Gold flow, each built to learn a different orchestration pattern:

### retail_daily_pipeline (Priority 1)
- **Orchestrator:** Azure Data Factory + Databricks Workflow
- **Trigger:** ADF Storage Event (fires when CSV lands in ADLS) + Daily 06:00 schedule
- **ADF activities:** `Get_PAT_Token` → `Copy_CSV_to_Landing` → `Trigger_Databricks_Job`
- **Tasks:** `bronze_ingest` → `silver_transform` → `gold`
- **Silver write:** full overwrite (`mode=overwrite`)
- **Runtime:** ~1m 17s

### retail_pipeline_v2 (Priority 6)
- **Orchestrator:** Databricks Workflows only (no ADF)
- **Trigger:** Daily 06:00 AM America/New_York
- **Tasks:** `bronze_ingest` → `silver_upsert` → `gold_aggregate` (with `Depends on:` chaining)
- **Silver write:** Delta MERGE upsert — existing rows updated, new rows inserted
- **Retries:** 3 attempts per task, 5-minute interval
- **Alerts:** Email on Start + Failure
- **Runtime:** ~90s

### retail_dlt_pipeline (Priority 9)
- **Orchestrator:** Delta Live Tables engine (automatic)
- **Type:** ETL pipeline (Lakeflow Pipelines)
- **Tables:** `bronze_sales` (Streaming), `silver_sales` (Streaming), `gold_sales_daily` (Materialized view)
- **Data quality:** `@dlt.expect` rules on Silver — tracked per run automatically
- **Lineage:** Bronze → Silver → Gold diagram auto-generated
- **Target:** `adb_retail_dev.dlt_demo` schema

---

## Stack

| Layer | Technology |
|---|---|
| Ingestion | Azure Data Factory · Auto Loader (cloudFiles) |
| Storage | Azure Data Lake Storage Gen2 (ADLS) |
| Compute | Azure Databricks (Serverless) |
| Table format | Delta Lake |
| Governance | Unity Catalog (`adb_retail_dev`) |
| Orchestration | ADF · Databricks Workflows · Delta Live Tables |
| Streaming | Structured Streaming (PySpark) |
| Analytics | Databricks SQL · Lakeflow Dashboard |
| Version control | GitHub (this repo) · Databricks Repos |
| Secrets | Azure Key Vault · Databricks Secret Scope |

---

## Unity Catalog Structure

```
adb_retail_dev (catalog)
├── bronze
│   ├── sales         (1,100 rows — raw ingested data)
│   └── run_log       (audit trail of every pipeline run)
├── silver
│   └── sales         (541 rows — cleaned, deduplicated, typed)
│                      All 11 columns documented with comments
├── gold
│   └── sales_daily   (15 rows — aggregated by store + date)
└── dlt_demo
    ├── bronze_sales       (DLT-managed streaming table)
    ├── silver_sales       (DLT-managed streaming table)
    └── gold_sales_daily   (DLT-managed materialized view)
```

---

## Key Concepts Demonstrated

**Delta Lake**
- MERGE upsert (`whenMatchedUpdate` + `whenNotMatchedInsertAll`)
- Time travel (`versionAsOf`, `timestampAsOf`, `RESTORE TABLE`)
- VACUUM with retention policy
- OPTIMIZE + Z-ORDER for query performance
- ANALYZE TABLE for statistics

**Data Quality**
- `try_to_date()` — safe date parsing, NULL for malformed values
- `row_number()` window deduplication before MERGE
- `@dlt.expect` / `@dlt.expect_or_drop` in DLT
- Idempotency testing (run twice, same result)

**Streaming**
- `readStream` from Delta table
- `writeStream` with `append` and `complete` output modes
- `trigger(availableNow=True)` for one-shot batch-style streaming
- `approx_count_distinct()` for streaming aggregations

**Git Workflow**
- 10 feature branches created and merged
- PR-based workflow (feature → PR → merge to main)
- 16+ commits with descriptive messages

---

## Workspace

```
Workspace : adb-7405605652873910.10.azuredatabricks.net
Catalog   : adb_retail_dev
Compute   : Serverless (all notebooks and pipelines)
```

---

## Dashboard

**Retail Sales Dashboard** — built in Databricks Dashboards (Genie Code)
- Bar chart: Revenue by store (S01 leads at ~$43K)
- Line chart: Daily revenue trend
- Bar chart: Top 5 products by revenue
- KPI tiles: Total Revenue $99,391 · Orders 111 · Customers 82
- Refreshes daily at 07:00 AM

---

## Interview Summary

> "I built a production-grade Azure Databricks pipeline: ADF + Auto Loader for Bronze ingestion, Delta MERGE upsert for Silver with data quality checks, Gold aggregation with Z-ORDER optimization, full Git workflow with feature branches and PRs, Unity Catalog governance with column documentation, Databricks Workflows for orchestration with retries and alerts, Structured Streaming for real-time processing, Delta Live Tables for declarative pipeline with expectations, and a Databricks SQL dashboard refreshing daily."
