-- Ingestion Config (source -> raw)

CREATE TABLE ingestion_config (
    table_name VARCHAR(100),
    source_path VARCHAR(200),
    target_path VARCHAR(200)
);

-- Insert values

INSERT INTO ingestion_config VALUES
('orders', 'ecommerce/orders.csv', 'ecommerce/orders'),
('customers', 'ecommerce/customers.csv', 'ecommerce/customers'),
('products', 'ecommerce/products.csv', 'ecommerce/products'),
('sellers', 'ecommerce/sellers.csv', 'ecommerce/sellers'),
('order_items', 'ecommerce/order_items.csv', 'ecommerce/order_items');

-- Serving Config (gold -> sql db)

CREATE TABLE serving_config (
    table_name VARCHAR(100),
    source_path VARCHAR(200)
);

-- Insert values

INSERT INTO serving_config VALUES
('dim_customers', 'ecommerce/dim_customers'),
('dim_products', 'ecommerce/dim_products'),
('dim_sellers', 'ecommerce/dim_sellers'),
('dim_orders', 'ecommerce/dim_orders'),
('fact_order_items', 'ecommerce/fact_order_items');
