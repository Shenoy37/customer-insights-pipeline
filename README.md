# Customer Insights Pipeline

A medallion-architecture data pipeline built on **Azure Databricks**, **Auto Loader**, **Delta Lake**, and **Unity Catalog**. It ingests raw customer, order, product, and region files, incrementally lands them in a Bronze layer, cleans and enriches them in a Silver layer, and models a customer dimension (SCD Type 1) in a Gold layer for downstream analytics.

## Architecture

```
        Source (ADLS Gen2: source/*.csv)
                 |
                 v
   ┌─────────────────────────────┐
   │  BRONZE  (raw, incremental) │  Auto Loader (cloudFiles) → Parquet
   └─────────────────────────────┘
                 |
                 v
   ┌─────────────────────────────┐
   │  SILVER  (cleaned, modeled) │  Cleansing, enrichment, UDFs → Delta
   └─────────────────────────────┘
                 |
                 v
   ┌─────────────────────────────┐
   │  GOLD    (business-ready)   │  Dimension modeling, SCD Type 1 → Delta
   └─────────────────────────────┘
```

Each layer is backed by its own container in Azure Data Lake Storage Gen2 (`source`, `bronze`, `silver`, `gold` under the storage account `azuredatabricksete`) and registered as managed tables in the Unity Catalog catalog `catalog_databricks_ete`.

## Repository Structure

```
customer-insights-pipeline/
├── data/                        # Sample source datasets (used to simulate incremental drops)
│   ├── customer_first.parquet       # Initial customer batch (1,990 rows)
│   ├── customers_second.parquet     # Incremental customer batch (10 rows)
│   ├── orders_first.parquet         # Initial orders batch (9,990 rows)
│   ├── orders_second.parquet        # Incremental orders batch (10 rows)
│   ├── products_first.parquet       # Initial products batch (490 rows)
│   ├── products_second.parquet      # Incremental products batch (10 rows)
│   └── regions.parquet              # Static region lookup (4 rows)
│
├── bronze/
│   ├── paramters.ipynb          # Defines the list of datasets to process and passes them
│   │                             # to downstream tasks via dbutils.jobs.taskValues
│   └── bronze_layer.ipynb       # Parameterized Auto Loader notebook — incrementally reads
│                                  # CSVs from the source container and writes Parquet to Bronze
│
├── silver/
│   ├── silver_customers.ipynb   # Cleans customers, derives email domain, builds full_name,
│   │                              # aggregates customers by domain, writes Delta table
│   ├── silver_orders.ipynb      # Casts order_date, adds year column, demonstrates window
│   │                              # functions (rank/dense_rank/row_number) via an OOP helper
│   │                              # class, de-duplicates, writes Delta table
│   ├── silver_products.ipynb    # Registers Unity Catalog SQL & Python UDFs (discount price,
│   │                              # uppercase brand) and applies them to the products data
│   └── silver_regions.ipynb     # Cleans and writes the regions lookup as a Delta table
│
├── gold/
│   └── gold_customers.ipynb     # Builds DimCustomer: de-duplication, surrogate key
│                                  # generation, and Slowly Changing Dimension (SCD) Type 1
│                                  # merge logic for initial vs. incremental loads
│
├── LICENSE                      # MIT License
└── README.md
```

## Tech Stack

| Layer | Technology |
|---|---|
| Compute | Azure Databricks (PySpark, Spark SQL) |
| Ingestion | Databricks Auto Loader (`cloudFiles`), Structured Streaming |
| Storage | Azure Data Lake Storage Gen2 (ABFS) |
| Table format | Delta Lake |
| Governance | Unity Catalog (managed tables & SQL/Python UDFs) |
| Orchestration | Databricks Jobs / Workflows, notebook widgets & task values |

## Prerequisites

Before running this pipeline, make sure you have:

1. An **Azure Databricks workspace** with a running cluster or SQL warehouse.
2. An **Azure Data Lake Storage Gen2** account (referred to as `azuredatabricksete` in the notebooks) with four containers: `source`, `bronze`, `silver`, and `gold`.
3. A **Unity Catalog** catalog named `catalog_databricks_ete` with `bronze`, `silver`, and `gold` schemas, and the appropriate external locations/credentials configured against the storage account above.
4. Access enabled to run notebooks with `dbutils`, `spark.readStream`, and SQL/Python UDF creation.

> If you're standing this up in your own environment, replace `azuredatabricksete` and `catalog_databricks_ete` throughout the notebooks with your own storage account and catalog names.

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/Shenoy37/customer-insights-pipeline.git
cd customer-insights-pipeline
```

### 2. Upload the sample data

Upload the files in `data/` to the `source` container of your ADLS Gen2 account as CSVs (or adapt the Bronze notebook to read Parquet directly). Use the `*_first` files to simulate the initial full load and the `*_second` files to simulate a later incremental drop — this is what the Auto Loader / SCD logic is designed to demonstrate.

### 3. Import the notebooks into Databricks

Import the `bronze/`, `silver/`, and `gold/` folders into your Databricks workspace, preserving the folder structure.

### 4. Run the Bronze layer

1. Run **`bronze/paramters.ipynb`** first. It defines the list of source datasets (`orders`, `customers`, `products`, `regions`) and publishes them as a task value for a Databricks Job to loop over.
2. Run **`bronze/bronze_layer.ipynb`** for each dataset, passing the dataset name via the `file_name` widget. This notebook uses Auto Loader to incrementally stream new CSV files from `source/{file_name}` into `bronze/{file_name}` as Parquet, tracking schema evolution and checkpoints.

   To orchestrate this automatically across all four datasets, wire the two notebooks into a **Databricks Job** where a "for-each" task loops over the values published in step 1 and calls `bronze_layer.ipynb` once per dataset.

### 5. Run the Silver layer

Run each notebook in `silver/` (order doesn't matter between entities, but each depends on its Bronze counterpart having run first):

- `silver_customers.ipynb` → writes `catalog_databricks_ete.silver.customers_silver`
- `silver_orders.ipynb` → writes the cleaned, de-duplicated orders Delta table
- `silver_products.ipynb` → applies the registered discount and brand-formatting UDFs
- `silver_regions.ipynb` → writes the regions Delta table

### 6. Run the Gold layer

Run **`gold/gold_customers.ipynb`** to build the `DimCustomer` table:

- Pass `init_load_flag = 1` on the very first run (full/initial load — no history to compare against).
- Pass `init_load_flag = 0` on subsequent runs (incremental load — the notebook reads the existing `DimCustomer` table, splits incoming records into new vs. existing, assigns surrogate keys continuing from the current maximum, and applies SCD Type 1 logic).

## Data Model

| Dataset | Key Columns | Description |
|---|---|---|
| `customers` | `customer_id`, `first_name`, `last_name`, `email`, `city`, `state` | Customer master data |
| `orders` | `order_id`, `customer_id`, `product_id`, `order_date`, `quantity`, `total_amount` | Transactional order records |
| `products` | `product_id`, `product_name`, `category`, `brand`, `price` | Product catalog |
| `regions` | `region_id`, `region` | Static region lookup |

## Current State & Next Steps

This project is a work in progress. A few things to be aware of if you're navigating or extending it:
- Only a customer dimension exists in Gold so far; Gold fact/dimension tables for orders and products are natural extensions.

## License

This project is licensed under the [MIT License](LICENSE) © 2026 Siddharth Shenoy.
