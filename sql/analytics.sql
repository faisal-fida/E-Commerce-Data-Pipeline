-- 1. Monthly sales per product category per region
SELECT 
    d.year,
    d.month,
    p.category,
    c.customer_region,
    SUM(f.quantity) as total_quantity,
    SUM(f.total_amount) as total_sales
FROM FACT_ORDERS f
JOIN DIM_DATE d ON f.date_id = d.date_id
JOIN DIM_PRODUCT p ON f.product_id = p.product_id
JOIN DIM_CUSTOMER c ON f.customer_id = c.customer_id
GROUP BY d.year, d.month, p.category, c.customer_region
ORDER BY d.year, d.month, p.category, c.customer_region;

-- 2. Top 5 repeat customers in the last 30 days
WITH customer_orders AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        COUNT(*) as order_count
    FROM FACT_ORDERS f
    JOIN DIM_CUSTOMER c ON f.customer_id = c.customer_id
    WHERE f.order_timestamp >= DATEADD(day, -30, CURRENT_TIMESTAMP())
    GROUP BY c.customer_id, c.customer_name
)
SELECT 
    customer_id,
    customer_name,
    order_count
FROM customer_orders
ORDER BY order_count DESC
LIMIT 5;

-- 3. Total quantity sold and average price per product
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    SUM(f.quantity) as total_quantity_sold,
    AVG(f.total_amount / f.quantity) as average_price
FROM FACT_ORDERS f
JOIN DIM_PRODUCT p ON f.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_quantity_sold DESC;

-- 4. Customers who placed orders in multiple regions
WITH customer_regions AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        COUNT(DISTINCT c.customer_region) as region_count
    FROM FACT_ORDERS f
    JOIN DIM_CUSTOMER c ON f.customer_id = c.customer_id
    GROUP BY c.customer_id, c.customer_name
)
SELECT 
    customer_id,
    customer_name,
    region_count
FROM customer_regions
WHERE region_count > 1
ORDER BY region_count DESC;

-- Bonus: Handling Slowly Changing Dimensions (SCD)
-- This query shows how to track changes in customer information over time
SELECT 
    c.customer_id,
    c.customer_name,
    c.customer_region,
    c.created_at,
    c.updated_at,
    COUNT(*) OVER (PARTITION BY c.customer_id) as version_count
FROM DIM_CUSTOMER c
ORDER BY c.customer_id, c.updated_at DESC; 