-- Moment 3 (DWH): model gwiazdy + log uruchomien ETL.
CREATE TABLE IF NOT EXISTS etl.etl_run_log (
    run_id BIGSERIAL PRIMARY KEY,
    run_date DATE NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    status TEXT NOT NULL,
    rows_loaded BIGINT DEFAULT 0,
    error_message TEXT
);

-- Wymiar daty wykorzystywany do agregacji czasowych.
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_id BIGSERIAL PRIMARY KEY,
    date_day DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    day_of_week INT NOT NULL
);

-- Wymiar kanalu sprzedazy.
CREATE TABLE IF NOT EXISTS dwh.dim_channel (
    channel_id BIGSERIAL PRIMARY KEY,
    channel_name TEXT NOT NULL UNIQUE
);

-- Wymiar geograficzny klienta.
CREATE TABLE IF NOT EXISTS dwh.dim_geography (
    geography_id BIGSERIAL PRIMARY KEY,
    country TEXT,
    city TEXT,
    postal_code TEXT,
    UNIQUE(country, city, postal_code)
);

-- Wymiar klienta (SCD uproszczony przez upsert).
CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_id BIGSERIAL PRIMARY KEY,
    customer_nk TEXT NOT NULL UNIQUE,
    email TEXT,
    phone TEXT,
    segment TEXT,
    geography_id BIGINT REFERENCES dwh.dim_geography(geography_id),
    updated_at TIMESTAMP DEFAULT now()
);

-- Wymiar produktu.
CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_id BIGSERIAL PRIMARY KEY,
    part_sku TEXT NOT NULL UNIQUE,
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
    updated_at TIMESTAMP DEFAULT now()
);

-- Fakt naglowkow zamowien.
CREATE TABLE IF NOT EXISTS dwh.fact_orders (
    order_id BIGSERIAL PRIMARY KEY,
    order_nk TEXT NOT NULL UNIQUE,
    customer_id BIGINT NOT NULL REFERENCES dwh.dim_customer(customer_id),
    date_id BIGINT NOT NULL REFERENCES dwh.dim_date(date_id),
    channel_id BIGINT REFERENCES dwh.dim_channel(channel_id),
    order_status TEXT,
    currency TEXT,
    delivery_method TEXT,
    discount_amount NUMERIC(14,2),
    updated_at TIMESTAMP DEFAULT now()
);

-- Fakt pozycji zamowien z metrykami finansowymi.
CREATE TABLE IF NOT EXISTS dwh.fact_order_items (
    fact_order_item_id BIGSERIAL PRIMARY KEY,
    order_item_nk TEXT NOT NULL UNIQUE,
    order_id BIGINT NOT NULL REFERENCES dwh.fact_orders(order_id),
    product_id BIGINT NOT NULL REFERENCES dwh.dim_product(product_id),
    qty NUMERIC(14,3) NOT NULL,
    unit_price_net NUMERIC(14,2),
    unit_price_gross NUMERIC(14,2),
    discount_amount NUMERIC(14,2),
    tax_amount NUMERIC(14,2),
    gross_amount NUMERIC(14,2),
    net_amount NUMERIC(14,2),
    margin_amount NUMERIC(14,2),
    updated_at TIMESTAMP DEFAULT now()
);

-- Indeksy wspierajace najczestsze joiny i analityke.
CREATE INDEX IF NOT EXISTS idx_fact_orders_date_id ON dwh.fact_orders(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_items_order_id ON dwh.fact_order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_fact_items_product_id ON dwh.fact_order_items(product_id);
