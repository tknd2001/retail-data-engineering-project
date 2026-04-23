CREATE TABLE fact_sales
WITH (
format = 'PARQUET',
external_location = 's3://retail-data-project-simulation/fact/sales/'
) AS
SELECT
oi.order_item_id,
oi.order_id,
oi.product_id,
o.customer_id,
o.store_id,
o.order_date,
EXTRACT(YEAR FROM o.order_date) AS order_year,
EXTRACT(MONTH FROM o.order_date) AS order_month,
oi.quantity,
oi.unit_price,
CAST(oi.quantity * oi.unit_price AS DOUBLE) AS sales_amount,
p.product_name,
p.category,
c.customer_name,
c.region AS customer_region,
s.store_name,
s.region AS store_region,
oi.load_date
FROM staging_order_items oi
JOIN staging_orders o
ON oi.order_id = o.order_id
JOIN staging_products p
ON oi.product_id = p.product_id
JOIN staging_customers c
ON o.customer_id = c.customer_id
JOIN staging_stores s
ON o.store_id = s.store_id;
