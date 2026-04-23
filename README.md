# retail-data-engineering-project
This project was about building an end-to-end data engineering pipeline in AWS that simulates a retail business environment. Raw transactional data flows through a fully automated validation, transformation and cataloguing pipeline before being made available for analytical querying via a star schema data model.

## Services Used
| Service | Purpose |
|---|---|
| **Amazon S3** | Layered data storage (raw, staging, rejected, audit) |
| **AWS Glue** | PySpark ETL jobs for validation and transformation |
| **Amazon Athena** | SQL querying and star schema modelling |
| **AWS Lambda** | Serverless crawler trigger |
| **Amazon EventBridge** | Event-driven pipeline automation |
| **AWS IAM** | Role-based access control |


```
s3://retail-data-project-simulation/
├── raw/
│   ├── customers/load_date=YYYY-MM-DD/
│   ├── products/load_date=YYYY-MM-DD/
│   ├── stores/load_date=YYYY-MM-DD/
│   ├── orders/load_date=YYYY-MM-DD/
│   └── order_items/load_date=YYYY-MM-DD/
├── staging/
│   ├── customers/load_date=YYYY-MM-DD/
│   ├── products/load_date=YYYY-MM-DD/
│   ├── stores/load_date=YYYY-MM-DD/
│   ├── orders/load_date=YYYY-MM-DD/
│   └── order_items/load_date=YYYY-MM-DD/
├── rejected/
│   ├── customers/load_date=YYYY-MM-DD/
│   ├── products/load_date=YYYY-MM-DD/
│   ├── stores/load_date=YYYY-MM-DD/
│   ├── orders/load_date=YYYY-MM-DD/
│   └── order_items/load_date=YYYY-MM-DD/
├── audit/
│   ├── customers/load_date=YYYY-MM-DD/
│   ├── products/load_date=YYYY-MM-DD/
│   ├── stores/load_date=YYYY-MM-DD/
│   ├── orders/load_date=YYYY-MM-DD/
│   └── order_items/load_date=YYYY-MM-DD/
├── dim/
│   ├── customers/
│   ├── date/
│   ├── order_items/
│   ├── orders/
│   ├── products/
│   └── stores/
└── fact/
    └── sales/
```

### IAM Role Setup
Create an IAM role named `GlueETLRoleS3Access` with the following policies attached:

| Policy | Purpose |
|---|---|
| `AmazonS3FullAccess` | Allows Glue jobs to read from and write to all S3 layers |
| `AWSGlueConsoleFullAccess` | Allows full Glue console access including crawlers and jobs |
| `AWSGlueServiceRole` | Grants Glue the baseline permissions needed to execute jobs |

This role must be attached to:
- All five Glue validation jobs
- The Glue Crawler (`retail-raw-crawler`)

### S3 Bucket
Create an S3 bucket named `retail-data-project-simulation` with the folder 
structure outlined in the S3 Folder Structure section below. The raw layer 
must be populated with source CSV files before running the pipeline.

### Source Data
The following CSV files must be uploaded to their respective raw S3 paths 
before executing the Glue jobs:

| File | S3 Path |
|---|---|
| `customers.csv` | `s3://retail-data-project-simulation/raw/customers/load_date=YYYY-MM-DD/` |
| `products.csv` | `s3://retail-data-project-simulation/raw/products/load_date=YYYY-MM-DD/` |
| `stores.csv` | `s3://retail-data-project-simulation/raw/stores/load_date=YYYY-MM-DD/` |
| `orders.csv` | `s3://retail-data-project-simulation/raw/orders/load_date=YYYY-MM-DD/` |
| `order_items.csv` | `s3://retail-data-project-simulation/raw/order_items/load_date=YYYY-MM-DD/` |

## How to Run the Pipeline

### Step 1 — Upload Source Data
Upload the following CSV files to their respective raw S3 paths, replacing 
`YYYY-MM-DD` with today's date:

| File | S3 Path |
|---|---|
| `customers.csv` | `s3://retail-data-project-simulation/raw/customers/load_date=YYYY-MM-DD/` |
| `products.csv` | `s3://retail-data-project-simulation/raw/products/load_date=YYYY-MM-DD/` |
| `stores.csv` | `s3://retail-data-project-simulation/raw/stores/load_date=YYYY-MM-DD/` |
| `orders.csv` | `s3://retail-data-project-simulation/raw/orders/load_date=YYYY-MM-DD/` |
| `order_items.csv` | `s3://retail-data-project-simulation/raw/order_items/load_date=YYYY-MM-DD/` |

---

### Step 2 — Run Glue Validation Jobs
Navigate to **AWS Glue → ETL Jobs** and run the following jobs in this order:

1. `Customers Validation`
2. `Products Validation`
3. `Stores Validation`
4. `Orders Validation`
5. `Orders_items Validation`

Each job will:
- Read raw CSV data from S3
- Apply data quality rules
- Write valid records to the staging layer
- Write invalid records to the rejected layer
- Write an audit log regardless of success or failure

> **Note:** Orders and order_items depend on upstream reference data. 
> Always run Customers, Products and Stores first.

---

### Step 3 — Verify Audit Logs
After each job completes, verify the audit logs at:
```
s3://retail-data-project-simulation/audit/{dataset}/load_date=YYYY-MM-DD/
```
Each log will contain total rows, valid rows, rejected rows, rejection reasons 
and job status.

---

### Step 4 — Partition Discovery (Automated)
Once any validation job completes successfully, the following automation 
chain triggers automatically:

```
Glue validation job SUCCEEDS
→ EventBridge detects job completion
→ Triggers start_crawler Lambda function
→ Lambda invokes retail-raw-crawler
→ Crawler scans s3://retail-data-project-simulation/raw/
→ New load_date partitions registered in Glue Data Catalog
→ Athena immediately sees new data
```

This eliminates the need to manually run `MSCK REPAIR TABLE` after each 
pipeline execution. Every time a new `load_date` partition lands in S3, 
whether from a Glue job or a manual file upload, Athena will automatically 
be able to query it once the crawler completes.

No manual intervention is required.
---

### Step 5 — Query in Athena
Navigate to **Amazon Athena** and run queries against the star schema, for testing purposes. Results will be provided in a seperate folder.

```sql
-- Sales by Category
SELECT category, SUM(sales_amount)
FROM fact_sales
GROUP BY category;

-- Monthly Trend
SELECT order_year, order_month, SUM(sales_amount)
FROM fact_sales
GROUP BY order_year, order_month;

-- Top Products
SELECT product_name, SUM(sales_amount)
FROM fact_sales
GROUP BY product_name
ORDER BY 2 DESC;
```

## Data Quality Rules

All five Glue validation jobs enforce the following rules. Valid records are 
promoted to the staging layer, invalid records are written to the rejected 
layer with a descriptive error reason captured in the audit log.

---

### Customers Validation

| # | Rule | Detail |
|---|---|---|
| 1 | **Required columns exist** | `customer_id`, `customer_name`, `city`, `state`, `signup_date` must all be present in the file |
| 2 | **No nulls or blanks** | `customer_id`, `customer_name` and `signup_date` cannot be null or empty |
| 3 | **Valid signup_date format** | `signup_date` must match `yyyy-MM-dd` format |
| 4 | **No duplicate PKs** | `customer_id` must be unique — first occurrence kept, duplicates rejected |

---

### Products Validation

| # | Rule | Detail |
|---|---|---|
| 1 | **Required columns exist** | `product_id`, `product_name`, `category`, `price` must all be present in the file |
| 2 | **No nulls or blanks** | `product_id`, `product_name` and `price` cannot be null or empty |
| 3 | **Valid price data type** | `price` must be castable to double |
| 4 | **Price range** | `price` must be > 0 |
| 5 | **No duplicate PKs** | `product_id` must be unique — first occurrence kept, duplicates rejected |

---

### Stores Validation

| # | Rule | Detail |
|---|---|---|
| 1 | **Required columns exist** | `store_id`, `store_name`, `city`, `state`, `region` must all be present in the file |
| 2 | **No nulls or blanks** | `store_id`, `store_name` and `region` cannot be null or empty |
| 3 | **No duplicate PKs** | `store_id` must be unique — first occurrence kept, duplicates rejected |

---

### Orders Validation

| # | Rule | Detail |
|---|---|---|
| 1 | **Required columns exist** | `order_id`, `customer_id`, `store_id`, `order_date` must all be present in the file |
| 2 | **No nulls or blanks** | All four required columns cannot be null or empty |
| 3 | **Valid order_date format** | `order_date` must match `yyyy-MM-dd` format |
| 4 | **No duplicate PKs** | `order_id` must be unique — first occurrence kept, duplicates rejected |
| 5 | **Foreign key: customer_id** | `customer_id` must exist in the customers dataset |
| 6 | **Foreign key: store_id** | `store_id` must exist in the stores dataset |

---

### Orders_items Validation

| # | Rule | Detail |
|---|---|---|
| 1 | **Required columns exist** | `order_item_id`, `order_id`, `product_id`, `quantity`, `unit_price` must all be present in the file |
| 2 | **No nulls or blanks** | All five required columns cannot be null or empty |
| 3 | **Valid quantity data type** | `quantity` must be castable to integer |
| 4 | **Quantity range** | `quantity` must be > 0 |
| 5 | **Valid unit_price data type** | `unit_price` must be castable to double |
| 6 | **Unit price range** | `unit_price` must be > 0 |
| 7 | **No duplicate PKs** | `order_item_id` must be unique — first occurrence kept, duplicates rejected |
| 8 | **Foreign key: order_id** | `order_id` must exist in the orders dataset |
| 9 | **Foreign key: product_id** | `product_id` must exist in the products dataset |

---

### Additional Safeguards Applied Across All Jobs

| Safeguard | Detail |
|---|---|
| **Empty file detection** | Jobs raise a descriptive exception if the source file contains no rows, captured in the audit log as FAILED |
| **Missing file handling** | S3 path failures are caught by the try/except block and logged to audit with a FAILED status and descriptive error message |
| **Schema mismatch detection** | Jobs explicitly check for required columns before any row-level processing begins |
