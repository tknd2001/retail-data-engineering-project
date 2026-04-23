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
