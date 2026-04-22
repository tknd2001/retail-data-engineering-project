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

