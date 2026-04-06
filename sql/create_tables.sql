-- Dimension Tables

CREATE TABLE dim_customers (
    customer_id VARCHAR(50),
    customer_unique_id VARCHAR(50),
    customer_city VARCHAR(100),
    customer_state VARCHAR(10)
);

CREATE TABLE dim_products (
    product_id VARCHAR(50),
    product_category_name VARCHAR(100),
    product_name_length INT,
    product_description_length INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

CREATE TABLE dim_sellers (
    seller_id VARCHAR(50),
    seller_city VARCHAR(100),
    seller_state VARCHAR(10)
);

CREATE TABLE dim_orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(50),
    order_purchase_timestamp DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,
);

-- Fact Table

CREATE TABLE fact_order_items (
    order_id VARCHAR(50),
    order_item_id INT,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_purchase_timestamp DATETIME,
    price FLOAT,
    freight_value FLOAT
);
