CREATE TABLE dim_products
WITH (
    format = 'PARQUET',
    external_location = 's3://retail-data-project-simulation/dim/products/'
) AS
SELECT DISTINCT
    product_id,
    product_name,
    category
FROM staging_products;

CREATE TABLE dim_customers
WITH (
    format = 'PARQUET',
    external_location = 's3://retail-data-project-simulation/dim/customers/'
) AS
SELECT DISTINCT
    customer_id,
    customer_name,
    region
FROM staging_customers;

CREATE TABLE dim_stores
WITH (
    format = 'PARQUET',
    external_location = 's3://retail-data-project-simulation/dim/stores/'
) AS
SELECT DISTINCT
    store_id,
    store_name,
    region
FROM staging_stores;

CREATE TABLE dim_date
WITH (
    format = 'PARQUET',
    external_location = 's3://retail-data-project-simulation/dim/date/'
) AS
SELECT DISTINCT
    order_date AS date,
    EXTRACT(YEAR FROM order_date) AS year,
    EXTRACT(MONTH FROM order_date) AS month,
    EXTRACT(DAY FROM order_date) AS day
FROM staging_orders;
