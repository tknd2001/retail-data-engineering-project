CREATE EXTERNAL TABLE staging_products (
    product_id STRING,
    product_name STRING,
    category STRING,
    price DECIMAL(10,0),
    load_date DATE
)
STORED AS PARQUET
LOCATION 's3://retail-data-project-simulation/staging/products/load_date=2026-04-16/';

CREATE EXTERNAL TABLE staging_customers (
customer_id STRING,
customer_name STRING,
region STRING,
load_date DATE
)
STORED AS PARQUET
LOCATION 's3://retail-data-project-simulation/staging/customers/load_date=2026-04-16/';

CREATE EXTERNAL TABLE staging_stores (
store_id STRING,
store_name STRING,
region STRING,
load_date DATE
)
STORED AS PARQUET
LOCATION 's3://retail-data-project-simulation/staging/stores/load_date=2026-04-16/';

CREATE EXTERNAL TABLE staging_orders (
order_id STRING,
customer_id STRING,
store_id STRING,
order_date DATE,
load_date DATE
)
STORED AS PARQUET
LOCATION 's3://retail-data-project-simulation/staging/orders/load_date=2026-04-16/';

CREATE EXTERNAL TABLE staging_order_items (
    order_item_id STRING,
    order_id STRING,
    product_id STRING,
    quantity INT,
    unit_price DOUBLE,
    load_date DATE
)
STORED AS PARQUET
LOCATION 's3://retail-data-project-simulation/staging/order_items/load_date=2026-04-16/';
