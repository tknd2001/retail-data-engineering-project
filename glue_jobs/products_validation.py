import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, when, trim, lit, monotonically_increasing_id, row_number
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

source_path = "s3://retail-data-project-simulation/raw/products/"
staging_path = f"s3://retail-data-project-simulation/staging/products/load_date={load_date}/"
rejected_path = f"s3://retail-data-project-simulation/rejected/products/load_date={load_date}/"
audit_path = f"s3://retail-data-project-simulation/audit/products/load_date={load_date}/"

status = "SUCCESS"
error_message = None
total_rows = 0
valid_rows = 0
invalid_rows = 0
invalid_reason_summary = ""

try:
    # Read CSV
    df = spark.read.option("header", True).csv(source_path)

    # Required columns
    required_cols = ["product_id", "product_name", "category", "price"]
    missing_cols = [c for c in required_cols if c not in df.columns]

    if missing_cols:
        raise Exception(f"Missing required columns: {missing_cols}")
        
    if df.count() == 0:
        raise Exception("Source file is empty")

    # Add helper row id so first-seen record is preserved among duplicates
    df = df.withColumn("row_id", monotonically_increasing_id())

    # Add error_reason column
    df = df.withColumn("error_reason", lit(None).cast("string"))

    # Mandatory fields not null / blank
    df = df.withColumn(
        "error_reason",
        when(
            col("error_reason").isNull() &
            (col("product_id").isNull() | (trim(col("product_id")) == "")),
            "product_id is null"
        ).otherwise(col("error_reason"))
    )

    df = df.withColumn(
        "error_reason",
        when(
            col("error_reason").isNull() &
            (col("product_name").isNull() | (trim(col("product_name")) == "")),
            "product_name is null"
        ).otherwise(col("error_reason"))
    )

    df = df.withColumn(
        "error_reason",
        when(
            col("error_reason").isNull() &
            (col("price").isNull() | (trim(col("price")) == "")),
            "price is null"
        ).otherwise(col("error_reason"))
    )

    # Cast price
    df = df.withColumn("price_num", col("price").cast("double"))

    # Invalid price type
    df = df.withColumn(
        "error_reason",
        when(
            col("error_reason").isNull() &
            col("price").isNotNull() &
            (trim(col("price")) != "") &
            col("price_num").isNull(),
            "invalid price data type"
        ).otherwise(col("error_reason"))
    )

    # price must be > 0
    df = df.withColumn(
        "error_reason",
        when(
            col("error_reason").isNull() &
            (col("price_num") <= 0),
            "price must be > 0"
        ).otherwise(col("error_reason"))
    )

    # Duplicate handling: keep first occurrence, reject later ones
    window_spec = Window.partitionBy("product_id").orderBy("row_id")

    df = df.withColumn("row_num", row_number().over(window_spec))

    df = df.withColumn(
        "error_reason",
        when(
            col("error_reason").isNull() &
            col("product_id").isNotNull() &
            (trim(col("product_id")) != "") &
            (col("row_num") > 1),
            "duplicate product_id"
        ).otherwise(col("error_reason"))
    )

    # Split valid / invalid
    valid_df = (
        df.filter(col("error_reason").isNull())
          .drop("error_reason", "price_num", "row_id", "row_num")
    )

    invalid_df = (
        df.filter(col("error_reason").isNotNull())
          .drop("price_num", "row_id", "row_num")
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

    # Write outputs
    valid_df.write.mode("overwrite").parquet(staging_path)
    invalid_df.write.mode("overwrite").parquet(rejected_path)

except Exception as e:
    status = "FAILED"
    error_message = str(e)

# Audit summary data
audit_data = [{
    "job_name": args["JOB_NAME"],
    "dataset": "products",
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
