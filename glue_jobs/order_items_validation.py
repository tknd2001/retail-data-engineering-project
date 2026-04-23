import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

source_path = "s3://retail-data-project-simulation/raw/order_items/"
orders_source_path = "s3://retail-data-project-simulation/raw/orders/"
products_source_path = "s3://retail-data-project-simulation/raw/products/"
staging_path = f"s3://retail-data-project-simulation/staging/order_items/load_date={load_date}/"
rejected_path = f"s3://retail-data-project-simulation/rejected/order_items/load_date={load_date}/"
audit_path = f"s3://retail-data-project-simulation/audit/order_items/load_date={load_date}/"

status = "SUCCESS"
error_message = None
total_rows = 0
valid_rows = 0
invalid_rows = 0
invalid_reason_summary = ""

try:
    # Read main order_items CSV from S3
    df = spark.read.option("header", True).csv(source_path)

    # Read reference files for foreign key validation
    orders_df = spark.read.option("header", True).csv(orders_source_path)
    products_df = spark.read.option("header", True).csv(products_source_path)

    # 1. Required columns check
    required_cols = ["order_item_id", "order_id", "product_id", "quantity", "unit_price"]
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
    mandatory_cols = ["order_item_id", "order_id", "product_id", "quantity", "unit_price"]

    for c in mandatory_cols:
        df = df.withColumn(
            "error_reason",
            when(
                col("error_reason").isNull() &
                (col(c).isNull() | (trim(col(c)) == "")),
                f"{c} is null or blank"
            ).otherwise(col("error_reason"))
        )

    # 4. quantity data type validation
    df = df.withColumn("quantity_num", col("quantity").cast("int"))

    df = df.withColumn(
        "error_reason",
        when(
            col("error_reason").isNull() &
            col("quantity").isNotNull() &
            (trim(col("quantity")) != "") &
            col("quantity_num").isNull(),
            "invalid quantity data type"
        ).otherwise(col("error_reason"))
    )

    # 5. quantity must be greater than 0
    df = df.withColumn(
        "error_reason",
        when(
            col("error_reason").isNull() &
            (col("quantity_num") <= 0),
            "quantity must be greater than 0"
        ).otherwise(col("error_reason"))
    )

    # 6. unit_price data type validation
    df = df.withColumn("unit_price_num", col("unit_price").cast("double"))

    df = df.withColumn(
        "error_reason",
        when(
            col("error_reason").isNull() &
            col("unit_price").isNotNull() &
            (trim(col("unit_price")) != "") &
            col("unit_price_num").isNull(),
            "invalid unit_price data type"
        ).otherwise(col("error_reason"))
    )

    # 7. unit_price must be greater than 0
    df = df.withColumn(
        "error_reason",
        when(
            col("error_reason").isNull() &
            (col("unit_price_num") <= 0),
            "unit_price must be greater than 0"
        ).otherwise(col("error_reason"))
    )

    # 8. Duplicate handling: keep first occurrence, reject later ones
    window_spec = Window.partitionBy("order_item_id").orderBy("row_id")

    df = df.withColumn("row_num", row_number().over(window_spec))

    df = df.withColumn(
        "error_reason",
        when(
            col("error_reason").isNull() &
            col("order_item_id").isNotNull() &
            (trim(col("order_item_id")) != "") &
            (col("row_num") > 1),
            "duplicate order_item_id"
        ).otherwise(col("error_reason"))
    )

    # 9. Foreign key validation: order_id must exist in orders
    order_keys = (
        orders_df
        .select("order_id")
        .filter(col("order_id").isNotNull() & (trim(col("order_id")) != ""))
        .distinct()
        .withColumn("order_exists", lit(1))
    )

    df = df.join(order_keys, on="order_id", how="left")

    df = df.withColumn(
        "error_reason",
        when(
            col("error_reason").isNull() &
            col("order_id").isNotNull() &
            (trim(col("order_id")) != "") &
            col("order_exists").isNull(),
            "invalid order_id foreign key"
        ).otherwise(col("error_reason"))
    ).drop("order_exists")

    # 10. Foreign key validation: product_id must exist in products
    product_keys = (
        products_df
        .select("product_id")
        .filter(col("product_id").isNotNull() & (trim(col("product_id")) != ""))
        .distinct()
        .withColumn("product_exists", lit(1))
    )

    df = df.join(product_keys, on="product_id", how="left")

    df = df.withColumn(
        "error_reason",
        when(
            col("error_reason").isNull() &
            col("product_id").isNotNull() &
            (trim(col("product_id")) != "") &
            col("product_exists").isNull(),
            "invalid product_id foreign key"
        ).otherwise(col("error_reason"))
    ).drop("product_exists")

    # 11. Split valid and invalid rows
    valid_df = (
        df.filter(col("error_reason").isNull())
          .withColumn("quantity", col("quantity_num"))
          .withColumn("unit_price", col("unit_price_num"))
          .drop("quantity_num", "unit_price_num", "error_reason", "row_id", "row_num")
    )

    invalid_df = (
        df.filter(col("error_reason").isNotNull())
          .drop("quantity_num", "unit_price_num", "row_id", "row_num")
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

    # 12. Write valid rows to staging
    valid_df.write.mode("overwrite").parquet(staging_path)

    # 13. Write invalid rows to rejected
    invalid_df.write.mode("overwrite").parquet(rejected_path)

except Exception as e:
    status = "FAILED"
    error_message = str(e)

# Audit summary data
audit_data = [{
    "job_name": args["JOB_NAME"],
    "dataset": "order_items",
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
