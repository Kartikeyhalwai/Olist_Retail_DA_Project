USE CATALOG olist_retail;
USE SCHEMA gold_dataset;
CREATE OR REPLACE TABLE GOLD_DATASET.customer_behavior AS

-- NEW VS REPEAT CUSTOMER
SELECT
    customer_unique_id,
    COUNT(DISTINCT order_id) AS total_orders,

    CASE
        WHEN COUNT(DISTINCT order_id) = 1 THEN 'New'
        WHEN COUNT(DISTINCT order_id) > 1 THEN 'Repeat'
        ELSE 'Unknown'
    END AS customer_type

FROM GOLD_DATASET.fact_sales
GROUP BY customer_unique_id;