-- E-commerce Analytics Database Schema

-- Drop existing tables if they exist
DROP TABLE IF EXISTS daily_metrics CASCADE;
DROP TABLE IF EXISTS customer_metrics CASCADE;
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS data_sources CASCADE;
DROP TABLE IF EXISTS etl_logs CASCADE;

-- Data sources table
CREATE TABLE data_sources (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    type VARCHAR(20) NOT NULL CHECK (type IN ('shopify', 'woocommerce', 'commercetools')),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customers table
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    source_id VARCHAR(100) NOT NULL,
    source_type VARCHAR(20) NOT NULL,
    email VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    postal_code VARCHAR(20),
    total_spent DECIMAL(15, 2) DEFAULT 0,
    orders_count INTEGER DEFAULT 0,
    tags TEXT[],
    first_purchase_date DATE,
    last_purchase_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id, source_type)
);

-- Products table
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    source_id VARCHAR(100) NOT NULL,
    source_type VARCHAR(20) NOT NULL,
    title VARCHAR(255) NOT NULL,
    vendor VARCHAR(255),
    product_type VARCHAR(100),
    sku VARCHAR(100),
    price DECIMAL(15, 2),
    compare_at_price DECIMAL(15, 2),
    inventory_quantity INTEGER,
    tags TEXT[],
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id, source_type)
);

-- Orders table
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    source_id VARCHAR(100) NOT NULL,
    source_type VARCHAR(20) NOT NULL,
    order_number VARCHAR(100),
    customer_id INTEGER REFERENCES customers(id),
    email VARCHAR(255),
    financial_status VARCHAR(50),
    fulfillment_status VARCHAR(50),
    currency VARCHAR(10),
    subtotal_price DECIMAL(15, 2),
    total_tax DECIMAL(15, 2),
    total_discounts DECIMAL(15, 2),
    total_shipping DECIMAL(15, 2),
    total_price DECIMAL(15, 2),
    processed_at TIMESTAMP,
    cancelled_at TIMESTAMP,
    tags TEXT[],
    source_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id, source_type)
);

-- Order items table
CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id) ON DELETE CASCADE,
    product_id INTEGER REFERENCES products(id),
    source_product_id VARCHAR(100),
    source_variant_id VARCHAR(100),
    title VARCHAR(255),
    variant_title VARCHAR(255),
    sku VARCHAR(100),
    quantity INTEGER,
    price DECIMAL(15, 2),
    total_discount DECIMAL(15, 2),
    fulfillment_status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer metrics table for CLV and other calculations
CREATE TABLE customer_metrics (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id) ON DELETE CASCADE,
    calculation_date DATE NOT NULL,
    total_revenue DECIMAL(15, 2),
    total_orders INTEGER,
    average_order_value DECIMAL(15, 2),
    purchase_frequency DECIMAL(10, 4),
    customer_lifespan_days INTEGER,
    customer_lifetime_value DECIMAL(15, 2),
    churn_probability DECIMAL(5, 4),
    days_since_last_purchase INTEGER,
    rfm_recency_score INTEGER CHECK (rfm_recency_score BETWEEN 1 AND 5),
    rfm_frequency_score INTEGER CHECK (rfm_frequency_score BETWEEN 1 AND 5),
    rfm_monetary_score INTEGER CHECK (rfm_monetary_score BETWEEN 1 AND 5),
    customer_segment VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(customer_id, calculation_date)
);

-- Daily metrics aggregation table
CREATE TABLE daily_metrics (
    id SERIAL PRIMARY KEY,
    metric_date DATE NOT NULL,
    source_type VARCHAR(20),
    total_revenue DECIMAL(15, 2),
    total_orders INTEGER,
    total_customers INTEGER,
    new_customers INTEGER,
    returning_customers INTEGER,
    average_order_value DECIMAL(15, 2),
    total_products_sold INTEGER,
    top_selling_products JSONB,
    revenue_by_source JSONB,
    conversion_rate DECIMAL(5, 4),
    cart_abandonment_rate DECIMAL(5, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(metric_date, source_type)
);

-- ETL logs table for tracking pipeline runs
CREATE TABLE etl_logs (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    source_type VARCHAR(20),
    status VARCHAR(20) CHECK (status IN ('running', 'success', 'failed', 'warning')),
    records_extracted INTEGER DEFAULT 0,
    records_transformed INTEGER DEFAULT 0,
    records_loaded INTEGER DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    duration_seconds INTEGER,
    metadata JSONB
);

-- Create indexes for better performance
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_source ON customers(source_id, source_type);
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_processed_at ON orders(processed_at);
CREATE INDEX idx_orders_source ON orders(source_id, source_type);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);
CREATE INDEX idx_customer_metrics_customer ON customer_metrics(customer_id);
CREATE INDEX idx_customer_metrics_date ON customer_metrics(calculation_date);
CREATE INDEX idx_daily_metrics_date ON daily_metrics(metric_date);
CREATE INDEX idx_etl_logs_pipeline ON etl_logs(pipeline_name, started_at);

-- Create update timestamp trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply update trigger to relevant tables
CREATE TRIGGER update_customers_updated_at BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_products_updated_at BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_orders_updated_at BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_data_sources_updated_at BEFORE UPDATE ON data_sources
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();