import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, when, trim, lit, to_date, monotonically_increasing_id, row_number
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window

# Glue job setup
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Run metadata
load_date = datetime.now().strftime("%Y-%m-%d")
run_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

source_path = "s3://retail-data-project-simulation/raw/orders/"
customers_source_path = "s3://retail-data-project-simulation/raw/customers/"
stores_source_path = "s3://retail-data-project-simulation/raw/stores/"
staging_path = f"s3://retail-data-project-simulation/staging/orders/load_date={load_date}/"
rejected_path = f"s3://retail-data-project-simulation/rejected/orders/load_date={load_date}/"
audit_path = f"s3://retail-data-project-simulation/audit/orders/load_date={load_date}/"

status = "SUCCESS"
error_message = None
total_rows = 0
valid_rows = 0
invalid_rows = 0
invalid_reason_summary = ""

try:
    # Read orders CSV from S3
    df = spark.read.option("header", True).csv(source_path)

    # Read reference files for foreign key validation
    customers_df = spark.read.option("header", True).csv(customers_source_path)
    stores_df = spark.read.option("header", True).csv(stores_source_path)

    # 1. Required columns check
    required_cols = ["order_id", "customer_id", "store_id", "order_date"]
    missing_cols = [c for c in required_cols if c not in df.columns]

    if missing_cols:
        raise Exception(f"Missing required columns: {missing_cols}")
    
    if df.count() == 0:
        raise Exception("Source file is empty")

    # Add helper row id so first-seen record is preserved among duplicates
    df = df.withColumn("row_id", monotonically_increasing_id())

    # 2. Add error_reason column
    df = df.withColumn("error_reason", lit(None).cast("string"))

    # 3. Mandatory fields not null / blank
    mandatory_cols = ["order_id", "customer_id", "store_id", "order_date"]

    for c in mandatory_cols:
        df = df.withColumn(
            "error_reason",
            when(
                col("error_reason").isNull() &
                (col(c).isNull() | (trim(col(c)) == "")),
                f"{c} is null or blank"
            ).otherwise(col("error_reason"))
        )

    # 4. order_date format validation (expected yyyy-MM-dd)
    df = df.withColumn("order_date_parsed", to_date(col("order_date"), "yyyy-MM-dd"))

    df = df.withColumn(
        "error_reason",
        when(
            col("error_reason").isNull() &
            col("order_date").isNotNull() &
            (trim(col("order_date")) != "") &
            col("order_date_parsed").isNull(),
            "order_date format invalid, expected yyyy-MM-dd"
        ).otherwise(col("error_reason"))
    )

    # 5. Duplicate handling: keep first occurrence, reject later ones
    window_spec = Window.partitionBy("order_id").orderBy("row_id")

    df = df.withColumn("row_num", row_number().over(window_spec))

    df = df.withColumn(
        "error_reason",
        when(
            col("error_reason").isNull() &
            col("order_id").isNotNull() &
            (trim(col("order_id")) != "") &
            (col("row_num") > 1),
            "duplicate order_id"
        ).otherwise(col("error_reason"))
    )

    # 6. Foreign key validation: customer_id must exist in customers
    customer_keys = (
        customers_df
        .select("customer_id")
        .filter(col("customer_id").isNotNull() & (trim(col("customer_id")) != ""))
        .distinct()
        .withColumn("customer_exists", lit(1))
    )

    df = df.join(customer_keys, on="customer_id", how="left")

    df = df.withColumn(
        "error_reason",
        when(
            col("error_reason").isNull() &
            col("customer_id").isNotNull() &
            (trim(col("customer_id")) != "") &
            col("customer_exists").isNull(),
            "invalid customer_id foreign key"
        ).otherwise(col("error_reason"))
    ).drop("customer_exists")

    # 7. Foreign key validation: store_id must exist in stores
    store_keys = (
        stores_df
        .select("store_id")
        .filter(col("store_id").isNotNull() & (trim(col("store_id")) != ""))
        .distinct()
        .withColumn("store_exists", lit(1))
    )

    df = df.join(store_keys, on="store_id", how="left")

    df = df.withColumn(
        "error_reason",
        when(
            col("error_reason").isNull() &
            col("store_id").isNotNull() &
            (trim(col("store_id")) != "") &
            col("store_exists").isNull(),
            "invalid store_id foreign key"
        ).otherwise(col("error_reason"))
    ).drop("store_exists")

    # 8. Split valid and invalid rows
    valid_df = (
        df.filter(col("error_reason").isNull())
          .withColumn("order_date", col("order_date_parsed"))
          .drop("order_date_parsed", "error_reason", "row_id", "row_num")
    )

    invalid_df = (
        df.filter(col("error_reason").isNotNull())
          .drop("order_date_parsed", "row_id", "row_num")
    )

    # Counts for audit
    total_rows = int(df.count())
    valid_rows = int(valid_df.count())
    invalid_rows = int(invalid_df.count())

    # Invalid reason summary
    invalid_reason_rows = (
        invalid_df.groupBy("error_reason")
                  .count()
                  .collect()
    )

    invalid_reason_summary = "; ".join(
        [f"{row['error_reason']}: {row['count']}" for row in invalid_reason_rows]
    )

    # 9. Write valid rows to staging
    valid_df.write.mode("overwrite").parquet(staging_path)

    # 10. Write invalid rows to rejected
    invalid_df.write.mode("overwrite").parquet(rejected_path)

except Exception as e:
    status = "FAILED"
    error_message = str(e)

# Audit summary data
audit_data = [{
    "job_name": args["JOB_NAME"],
    "dataset": "orders",
    "load_date": load_date,
    "run_timestamp": run_timestamp,
    "source_path": source_path,
    "staging_path": staging_path,
    "rejected_path": rejected_path,
    "total_rows": total_rows,
    "valid_rows": valid_rows,
    "invalid_rows": invalid_rows,
    "invalid_reason_summary": invalid_reason_summary,
    "status": status,
    "error_message": error_message
}]

# Explicit schema for audit log
audit_schema = StructType([
    StructField("job_name", StringType(), True),
    StructField("dataset", StringType(), True),
    StructField("load_date", StringType(), True),
    StructField("run_timestamp", StringType(), True),
    StructField("source_path", StringType(), True),
    StructField("staging_path", StringType(), True),
    StructField("rejected_path", StringType(), True),
    StructField("total_rows", IntegerType(), True),
    StructField("valid_rows", IntegerType(), True),
    StructField("invalid_rows", IntegerType(), True),
    StructField("invalid_reason_summary", StringType(), True),
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True)
])

# Write audit summary
audit_df = spark.createDataFrame(audit_data, schema=audit_schema)
audit_df.coalesce(1).write.mode("overwrite").json(audit_path)

if status == "FAILED":
    raise Exception(error_message)

job.commit()
