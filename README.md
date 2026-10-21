# Retail Q — End-to-End Retail Data Pipeline

> A medallion architecture (bronze → silver → gold) pipeline that ingests retail data from multiple sources (Salesforce, PostgreSQL, Blob storage), applies CDC-based transformations with SCD Type 1/2, and delivers an analytics-ready gold layer with dashboard reporting.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Project Structure](#project-structure)
3. [Job Orchestration](#job-orchestration)
4. [Bronze Layer — Ingestion](#bronze-layer--ingestion)
5. [Silver Layer — CDC Transformations](#silver-layer--cdc-transformations)
6. [Gold Layer — Analytics](#gold-layer--analytics)
7. [Dashboard — Reporting](#dashboard--reporting)
8. [Data Quality Framework](#data-quality-framework)
9. [Genie Space — Conversational Analytics](#genie-space--conversational-analytics)

---

## Architecture Overview

The project follows a **medallion architecture** with three layers (bronze, silver, gold) orchestrated by a single Databricks Lakeflow Job:

```
                    ┌─────────────────────────────────────────────────────────────────┐
                    │                   Lakeflow Job: Retail_Q_project                  │
                    │                   (Job ID: 231763863485753)                      │
                    └────────────────────────────────────────────────────────────────-┘

  BRONZE LAYER (Parallel Ingestion)
  ┌──────────────────┐   ┌──────────────────────┐   ┌────────────────────────┐
  │  Blob_to_bronze   │   │ Postgres_to_bronze    │   │ Salesforce_to_bronze   │
  │  (Auto Loader)    │   │ (Lakeflow Connect)    │   │ (Lakeflow Connect)     │
  │  CSV → Delta      │   │ PostgreSQL → Delta    │   │ Salesforce → Delta     │
  └────────┬─────────┘   └──────────┬───────────┘   └───────────┬────────────┘
           │                        │                           │
           └────────────┬───────────┴───────────────────────────┘
                        ▼
  SILVER LAYER (CDC Transformations)
  ┌──────────────────────────────────────────────────────────────────┐
  │  Pipeline: Retail_silver_transformations                          │
  │  (ID: ded86ca2-aa13-4b1e-ad48-c729a027e67b)                      │
  │                                                                  │
  │  Temporary View → Streaming Table → Auto CDC Flow                │
  │  ┌────────────┐ ┌──────────────┐ ┌─────────────────┐            │
  │  │ account    │ │ opportunity  │ │ product_catalog │            │
  │  │ (SCD2)     │ │ (SCD1)       │ │ (SCD2)          │            │
  │  └────────────┘ └──────────────┘ └─────────────────┘            │
  │  ┌────────────┐ ┌──────────────────────────────────┐           │
  │  │ inventory  │ │ transactions (append-only stream)  │           │
  │  │ (SCD1)     │ │ (no CDC)                          │           │
  │  └────────────┘ └──────────────────────────────────┘           │
  └──────────────────────────┬───────────────────────────────────────┘
                             ▼
  GOLD LAYER (Analytics)
  ┌──────────────────────────────────────────────────────────────────┐
  │  fact_sales (Materialized View — batch join)                     │
  │  transactions ⋈ opportunity → enriched sales fact table          │
  └──────────────────────────┬───────────────────────────────────────┘
                             ▼
  REPORTING
  ┌──────────────────────────────────────────────────────────────────┐
  │  Retail Dashboard (AI/BI Lakeview)                               │
  │  Auto-subscribed on pipeline completion                          │
  └──────────────────────────────────────────────────────────────────┘
```

### Data Flow Summary

| Source | Bronze Table | Silver Table | SCD Type | Pattern |
|--------|-------------|-------------|----------|---------|
| Salesforce Account | `retail_q.salesforce_bronze.account` | `retail_q.retail_silver.account` | SCD2 | Temp View → ST → Auto CDC |
| Salesforce Opportunity | `retail_q.salesforce_bronze.opportunity` | `retail_q.retail_silver.opportunity` | SCD1 | Temp View → ST → Auto CDC |
| PostgreSQL Product Catalog | `retail_q.postgres_bronze.product_catalog` | `retail_q.retail_silver.product_catalog` | SCD2 | Temp View → ST → Auto CDC |
| PostgreSQL Inventory | `retail_q.postgres_bronze.inventory` | `retail_q.retail_silver.inventory` | SCD1 | Temp View → ST → Auto CDC |
| Blob Storage Transactions | `retail_q.blob_bronze.transactions` | `retail_q.retail_silver.transactions` | N/A | Streaming Table (append-only) |
| Silver (join) | — | `retail_q.retail_gold.fact_sales` | N/A | Materialized View (batch) |

---

## Project Structure

```
├── Retail_Q/                                    # Main project workspace
│   ├── Blob to Bronze Transaction Ingestion       # Auto Loader notebook (CSV → Delta)
│   ├── calendar                                   # Supplementary notebook (date dimension)
│   ├── Gold_views                                 # Supplementary notebook (gold explorations)
│   └── metric_view                                # Supplementary notebook (metric definitions)
│
├── Retail_silver_transformations/                # Spark Declarative Pipeline
│   ├── bronze_to_silver/                          # Silver layer transformations
│   │   ├── account.py                             # Account CDC (SCD2)
│   │   ├── opportunity.py                         # Opportunity CDC (SCD1)
│   │   ├── product_catalog.py                     # Product Catalog CDC (SCD2)
│   │   ├── inventory.py                            # Inventory CDC (SCD1)
│   │   └── transactions.py                         # Transactions streaming table
│   └── Silver_to_Gold/                            # Gold layer transformations
│       └── fact_sales.py                          # Fact sales materialized view
│
└── README.md                                     # This file
```

---

## Job Orchestration

The entire pipeline is orchestrated by a single Lakeflow Job:

| Property | Value |
|----------|-------|
| Job Name | `Retail_Q_project` |
| Job ID | `231763863485753` |
| Format | Multi-task |
| Max Concurrent Runs | 1 |
| Queue | Enabled |
| Performance Target | Performance Optimized |
| Trigger | Manual (user-triggered) |

### Task Dependency Graph

```
  Blob_to_bronze ─────────────────┐
  Postgres_to_bronze ─────────────┼──► Silver__Gold_transformations ──► Retail_dashbaord
  Salesforce_to_bronze ──────────┘
```

- **Tasks 1–3** (bronze ingestion) run in **parallel** — no inter-dependencies.
- **Task 4** (silver + gold) waits for all three bronze tasks to succeed.
- **Task 5** (dashboard) triggers after silver/gold completes.
- All tasks use `run_if: ALL_SUCCESS`.

---

## Bronze Layer — Ingestion

The bronze layer ingests raw data from three heterogeneous sources into Unity Catalog Delta tables. All bronze tables have **Change Data Feed (CDF)** enabled (`delta.enableChangeDataFeed = true`) to support downstream CDC processing.

### 1. Blob to Bronze — Transaction Ingestion

| Property | Value |
|----------|-------|
| Task Key | `Blob_to_bronze` |
| Type | Notebook task |
| Notebook | `Blob to Bronze Transaction Ingestion` |

**Description**: Uses Databricks **Auto Loader** (`cloudFiles`) to incrementally read transaction CSV files from a UC volume and write them into the bronze Delta table. The stream uses `Trigger.availableNow()` to process all pending files in a single batch per pipeline run.

| Config | Value |
|---------|-------|
| Source path | `/Volumes/retail_q/vol_blob/blob_source/transaction_source/` |
| Source format | CSV (header, schema inference) |
| Target table | `retail_q.blob_bronze.transactions` |
| Checkpoint | `/Volumes/retail_q/vol_blob/blob_source/_checkpoints/transactions` |
| Trigger | `availableNow` (batch per run) |
| CDC | None — append-only source |

### 2. Postgres to Bronze — Lakeflow Connect

| Property | Value |
|----------|-------|
| Task Key | `Postgres_to_bronze` |
| Type | Pipeline task (managed ingestion) |
| Pipeline ID | `45a533b4-ccce-4424-81d4-b79baa37024c` |
| Connection | `neon_postgres_retail` |
| Source type | PostgreSQL |
| Source catalog | `neondb` |
| Source schema | `public` |
| Destination schema | `retail_q.postgres_bronze` |
| Channel | PREVIEW |

**Tables ingested**:

| Source Table | Bronze Table | SCD Type | Cursor Column |
|-------------|-------------|----------|---------------|
| `product_catalog` | `retail_q.postgres_bronze.product_catalog` | SCD Type 2 | `updated_at` |
| `inventory` | `retail_q.postgres_bronze.inventory` | SCD Type 1 (default) | `last_stock_update` |

### 3. Salesforce to Bronze — Lakeflow Connect

| Property | Value |
|----------|-------|
| Task Key | `Salesforce_to_bronze` |
| Type | Pipeline task (managed ingestion) |
| Pipeline ID | `b1ef75fc-1b20-40e3-a84b-3098662b2a29` |
| Connection | `salesforce_retail_test` |
| Source type | Salesforce |
| Source schema | `objects` |
| Destination schema | `retail_q.salesforce_bronze` |
| Channel | CURRENT |

**Tables ingested**:

| Source Table | Bronze Table | SCD Type |
|-------------|-------------|----------|
| `Account` | `retail_q.salesforce_bronze.account` | SCD Type 2 |
| `Opportunity` | `retail_q.salesforce_bronze.opportunity` | SCD Type 1 (default) |

---

## Silver Layer — CDC Transformations

The silver layer is a **Spark Declarative Pipeline** (SDP) that applies incremental CDC transformations to bronze data, producing clean, business-ready Delta tables with data quality enforcement.

| Property | Value |
|----------|-------|
| Pipeline Name | `Retail_silver_transformations` |
| Pipeline ID | `ded86ca2-aa13-4b1e-ad48-c729a027e67b` |
| Task Key | `Silver__Gold_transformations` |
| Root path | `/Workspace/Users/davidlearning184@gmail.com/Retail_silver_transformations` |
| Libraries | `bronze_to_silver/**` (glob include) |
| Compute | Serverless, Photon |
| Edition | Advanced |
| Channel | Current |
| Publishing | `retail_q.retail_silver`, `retail_q.retail_gold` (fully qualified names) |

### Transformation Pattern

All CDC tables follow a consistent three-step pattern:

```
Step 1: Temporary View  →  reads bronze source via spark.readStream, applies transformations
Step 2: Streaming Table  →  creates target table with data quality expectations
Step 3: Auto CDC Flow    →  applies changes (inserts/updates/deletes) from temp view to target
```

This pattern (per Auto CDC best practices) uses temporary views for source preprocessing and `create_streaming_table()` + `create_auto_cdc_flow()` for the CDC target and flow.

### Silver Tables

#### 1. Account (`account.py`)

| Property | Value |
|----------|-------|
| Source | `retail_q.salesforce_bronze.account` |
| Target | `retail_q.retail_silver.account` |
| Primary key | `id` |
| Sequence column | `SystemModstamp` |
| SCD Type | **2** (full history with `__START_AT` / `__END_AT`) |
| Excluded columns | `SystemModstamp` (sequence-only, not in target) |

**Transformations**:
- `Name` → uppercased + trimmed as `customer_name`
- `Industry` → NULLs replaced with `"UNKNOWN"`
- All Salesforce `PascalCase` columns aliased to `snake_case`

**Data Quality**:
- `expect_all_or_drop`: `id IS NOT NULL`
- `expect_all`: `customer_name IS NOT NULL`

#### 2. Opportunity (`opportunity.py`)

| Property | Value |
|----------|-------|
| Source | `retail_q.salesforce_bronze.opportunity` |
| Target | `retail_q.retail_silver.opportunity` |
| Primary key | `id` |
| Sequence column | `SystemModstamp` |
| SCD Type | **1** (latest wins — upsert) |
| Excluded columns | `SystemModstamp` |

**Transformations**:
- `Amount` → categorized as `deal_size` (ENTERPRISE / MID_MARKET / SMALL)
- All Salesforce `PascalCase` columns aliased to `snake_case`

**Data Quality**:
- `expect_all_or_drop`: `id IS NOT NULL`
- `expect_all`: `name IS NOT NULL`, `amount >= 0`, `0 <= probability <= 100`, `stage_name IN ('Prospecting','Closed Won','Closed Lost')`

#### 3. Product Catalog (`product_catalog.py`)

| Property | Value |
|----------|-------|
| Source | `retail_q.postgres_bronze.product_catalog` |
| Target | `retail_q.retail_silver.product_catalog` |
| Primary key | `product_id` |
| Sequence column | `updated_at` |
| SCD Type | **2** (full history with `__START_AT` / `__END_AT`) |
| Excluded columns | `updated_at` |

**Transformations**:
- `product_id` → uppercased + trimmed
- `product_name` → initcapped + trimmed
- `category`, `subcategory`, `brand` → trimmed
- `supplier_name` → initcapped + trimmed
- `unit_price` → categorized as `price_segment` (Premium / High / Medium / Low / Basic)

**Data Quality**:
- `expect_all_or_fail`: `product_id IS NOT NULL`
- `expect_all_or_drop`: `product_name IS NOT NULL`, `category IS NOT NULL`, `brand IS NOT NULL`
- `expect_all`: `unit_price >= 0`, `launch_date IS NOT NULL`

#### 4. Inventory (`inventory.py`)

| Property | Value |
|----------|-------|
| Source | `retail_q.postgres_bronze.inventory` |
| Target | `retail_q.retail_silver.inventory` |
| Primary key | `inventory_id` |
| Sequence column | `Last_stock_update` |
| SCD Type | **1** (latest wins — upsert) |

**Transformations**:
- `stock_quantity` vs `reorder_level` → `inventory_status` ("low" / "ok")

**Data Quality**:
- `expect_all_or_drop`: `inventory_id IS NOT NULL`
- `expect_all`: `stock_quantity >= 0`, `product_id IS NOT NULL`, `store_id IS NOT NULL`

#### 5. Transactions (`transactions.py`)

| Property | Value |
|----------|-------|
| Source | `retail_q.blob_bronze.transactions` |
| Target | `retail_q.retail_silver.transactions` |
| Pattern | Streaming table (`@dp.table` + `spark.readStream`) — **no CDC** |

**Transformations**:
- `quantity`, `selling_price`, `discount_amount` → cast to `int`
- `gross_amount` → computed (`quantity * selling_price`)
- `transaction_timestamp` → parsed from `dd-MMM-yyyy hh.mm.ss a` format

**Data Quality**:
- `expect_or_drop`: `transaction_id IS NOT NULL`
- `expect`: `quantity > 0`, `selling_price >= 0`, `discount_amount >= 0`, `product_id IS NOT NULL`, `payment_mode IN ('UPI','Card','Cash','Net Banking')`

---

## Gold Layer — Analytics

### Fact Sales (`fact_sales.py`)

| Property | Value |
|----------|-------|
| Target | `retail_q.retail_gold.fact_sales` |
| Pattern | Materialized view (batch — `@dp.table` + `spark.read`) |
| Source tables | `retail_q.retail_silver.transactions` (left) ⋈ `retail_q.retail_silver.opportunity` (right) |
| Join type | Left outer join |
| Join key | `upper(trim(transaction.opportunity_name)) = upper(trim(opportunity.name))` |

**Output columns**:

| Column | Source |
|-------|--------|
| `transaction_id` | transactions |
| `opportunity_name` | transactions |
| `product_id` | transactions |
| `store_id` | transactions |
| `quantity` | transactions |
| `selling_price` | transactions |
| `discount_amount` | transactions |
| `transaction_timestamp` | transactions |
| `transaction_date` | derived (cast to date) |
| `payment_mode` | transactions |
| `sales_channel` | transactions |
| `name` | opportunity |
| `stage_name` | opportunity |
| `owner_id` | opportunity |
| `amount` | opportunity |
| `customer_id` | opportunity (`account_id`) |

---

## Dashboard — Reporting

| Property | Value |
|----------|-------|
| Task Key | `Retail_dashbaord` |
| Type | Dashboard task (AI/BI Lakeview) |
| Dashboard ID | `01f1c3ab5c6213febe90de3176b0e5b5` |
| SQL Warehouse | `18cc3c2b02256d8f` |
| Trigger | After silver/gold pipeline completes successfully |
| Subscription | Page attachment (auto-subscribed) |

The dashboard provides business stakeholders with retail sales analytics sourced from the gold layer.

---

## Data Quality Framework

Each silver table enforces data quality through Spark Declarative Pipeline expectations. Three severity levels are used:

| Severity | API | Behavior | Used For |
|----------|-----|----------|----------|
| **Fail** | `expect_all_or_fail` | Pipeline fails on violation | Critical keys (e.g., `product_id`) |
| **Drop** | `expect_all_or_drop` | Offending rows dropped; pipeline continues | Important keys (e.g., `id`, `name`) |
| **Warn** | `expect_all` | Rows retained; warning logged | Value range checks (e.g., `amount >= 0`) |

---

## Genie Space — Conversational Analytics

| Property | Value |
|----------|-------|
| Name | Retail Sales and Inventory Analytics |
| Type | AI/BI Genie Space |

**Description**: A conversational analytics agent that provides comprehensive insights into retail sales performance and inventory status, enabling data-driven decisions across customers, products, and time periods. Users can ask natural language questions to uncover trends, review performance, and analyze data without writing SQL.

**Capabilities**:

- Analyze sales trends by product, customer, time period, and sales channel
- Monitor inventory levels, stock updates, and reorder needs by product and warehouse location
- Assess customer segmentation, revenue contributions, and geographic distribution
- Evaluate product performance by category, brand, pricing, and supplier
- Track sales metrics including transaction amounts, discounts, payment modes, and stages

**Limitations**:

- Does not provide employee or operational cost analytics
- Lacks detailed information about supplier contracts or logistics operations
- Cannot analyze marketing campaign effectiveness or online engagement data

---

*Last updated: October 2026*