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
Once any validation job completes successfully, EventBridge automatically 
triggers the `start_crawler` Lambda function, which invokes the 
`retail-raw-crawler`. This registers any new `load_date` partitions in the 
Glue Data Catalog, making them immediately available in Athena.

No manual intervention is required.

---

### Step 5 — Query in Athena
Navigate to **Amazon Athena** and run queries against the star schema:

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
