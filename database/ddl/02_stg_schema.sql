-- Moment 2 (STG): dane po deduplikacji, castach i podstawowym data cleaningu.
CREATE TABLE IF NOT EXISTS stg.customers_clean (
    customer_id TEXT NOT NULL,
    email TEXT,
    phone TEXT,
    segment TEXT,
    country TEXT,
    city TEXT,
    postal_code TEXT,
    street TEXT,
    batch_date DATE NOT NULL,
    loaded_at TIMESTAMP DEFAULT now()
);

-- Oczyszczony katalog produktow.
CREATE TABLE IF NOT EXISTS stg.products_clean (
    part_sku TEXT NOT NULL,
    part_name TEXT,
    category TEXT,
    brand TEXT,
    manufacturer TEXT,
    oe_number TEXT,
    compatible_make TEXT,
    compatible_model TEXT,
    compatible_year_from INT,
    compatible_year_to INT,
    cost_net NUMERIC(14,2),
    price_net NUMERIC(14,2),
    batch_date DATE NOT NULL,
    loaded_at TIMESTAMP DEFAULT now()
);

-- Oczyszczone naglowki zamowien.
CREATE TABLE IF NOT EXISTS stg.orders_clean (
    order_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    order_created_at TIMESTAMP NOT NULL,
    order_date DATE NOT NULL,
    status TEXT NOT NULL,
    sales_channel TEXT,
    currency TEXT,
    discount_amount NUMERIC(14,2),
    delivery_method TEXT,
    batch_date DATE NOT NULL,
    loaded_at TIMESTAMP DEFAULT now()
);

-- Oczyszczone pozycje zamowien.
CREATE TABLE IF NOT EXISTS stg.order_items_clean (
    order_item_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    line_no INT,
    part_sku TEXT NOT NULL,
    qty NUMERIC(14,3) NOT NULL,
    unit_price_net NUMERIC(14,2),
    unit_price_gross NUMERIC(14,2),
    discount_amount NUMERIC(14,2),
    tax_amount NUMERIC(14,2),
    batch_date DATE NOT NULL,
    loaded_at TIMESTAMP DEFAULT now()
);

-- Indeksy przyspieszajace batchowe ladowanie faktow.
CREATE INDEX IF NOT EXISTS idx_stg_orders_clean_batch_date ON stg.orders_clean(batch_date);
CREATE INDEX IF NOT EXISTS idx_stg_order_items_clean_batch_date ON stg.order_items_clean(batch_date);
