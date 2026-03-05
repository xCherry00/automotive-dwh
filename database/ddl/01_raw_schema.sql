-- Moment 1 (RAW): surowe tabele landing zone na dane z CSV.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS etl;

-- Surowy snapshot klienta bez transformacji typow biznesowych.
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id TEXT,
    email TEXT,
    phone TEXT,
    segment TEXT,
    country TEXT,
    city TEXT,
    postal_code TEXT,
    street TEXT,
    source_updated_at TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT now(),
    batch_date DATE
);

-- Surowy katalog produktow.
CREATE TABLE IF NOT EXISTS raw.products (
    part_sku TEXT,
    part_name TEXT,
    category TEXT,
    brand TEXT,
    manufacturer TEXT,
    oe_number TEXT,
    compatible_make TEXT,
    compatible_model TEXT,
    compatible_year_from TEXT,
    compatible_year_to TEXT,
    cost_net TEXT,
    price_net TEXT,
    source_updated_at TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT now(),
    batch_date DATE
);

-- Surowe naglowki zamowien.
CREATE TABLE IF NOT EXISTS raw.orders (
    order_id TEXT,
    customer_id TEXT,
    order_created_at TEXT,
    status TEXT,
    sales_channel TEXT,
    delivery_method TEXT,
    discount_amount TEXT,
    currency TEXT,
    source_updated_at TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT now(),
    batch_date DATE
);

-- Surowe pozycje zamowien.
CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id TEXT,
    order_id TEXT,
    line_no TEXT,
    part_sku TEXT,
    qty TEXT,
    unit_price_net TEXT,
    unit_price_gross TEXT,
    discount_amount TEXT,
    tax_amount TEXT,
    source_updated_at TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT now(),
    batch_date DATE
);
