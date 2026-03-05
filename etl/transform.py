from __future__ import annotations

from pathlib import Path

from etl.utils import run_sql_file


def run_staging_transforms(conn, batch_date: str) -> None:
    # Moment STG: czyszczenie i standaryzacja rekordow z raw dla pojedynczego batcha.
    with conn.cursor() as cur:
        # Re-load batcha: usuwamy poprzedni wynik dla tej daty.
        cur.execute("DELETE FROM stg.orders_clean WHERE batch_date = %s::date", (batch_date,))
        cur.execute("DELETE FROM stg.order_items_clean WHERE batch_date = %s::date", (batch_date,))
        cur.execute("DELETE FROM stg.customers_clean WHERE batch_date = %s::date", (batch_date,))
        cur.execute("DELETE FROM stg.products_clean WHERE batch_date = %s::date", (batch_date,))

        # Orders: deduplikacja po order_id + normalizacja statusow i pol tekstowych.
        cur.execute(
            """
            INSERT INTO stg.orders_clean
            (order_id, customer_id, order_created_at, order_date, status, sales_channel, currency, discount_amount, delivery_method, batch_date)
            SELECT
                order_id,
                customer_id,
                order_created_at::timestamp,
                order_created_at::date,
                CASE lower(status)
                    WHEN 'new' THEN 'new'
                    WHEN 'paid' THEN 'paid'
                    WHEN 'shipped' THEN 'shipped'
                    WHEN 'returned' THEN 'returned'
                    WHEN 'cancelled' THEN 'cancelled'
                    ELSE 'new'
                END,
                lower(trim(sales_channel)),
                upper(trim(currency)),
                COALESCE(NULLIF(discount_amount, '')::numeric(14,2), 0),
                delivery_method,
                batch_date
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY COALESCE(source_updated_at, ingested_at) DESC) AS rn
                FROM raw.orders
                WHERE batch_date = %s::date
                  AND order_id IS NOT NULL
                  AND customer_id IS NOT NULL
                  AND order_created_at IS NOT NULL
            ) t
            WHERE rn = 1
            """,
            (batch_date,),
        )

        # Order items: deduplikacja po order_item_id i casty do typow numerycznych.
        cur.execute(
            """
            INSERT INTO stg.order_items_clean
            (order_item_id, order_id, line_no, part_sku, qty, unit_price_net, unit_price_gross, discount_amount, tax_amount, batch_date)
            SELECT
                order_item_id,
                order_id,
                line_no::int,
                part_sku,
                qty::numeric(14,3),
                unit_price_net::numeric(14,2),
                unit_price_gross::numeric(14,2),
                COALESCE(NULLIF(discount_amount, '')::numeric(14,2), 0),
                COALESCE(NULLIF(tax_amount, '')::numeric(14,2), 0),
                batch_date
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY order_item_id ORDER BY COALESCE(source_updated_at, ingested_at) DESC) AS rn
                FROM raw.order_items
                WHERE batch_date = %s::date
                  AND order_item_id IS NOT NULL
                  AND order_id IS NOT NULL
                  AND part_sku IS NOT NULL
                  AND qty::numeric > 0
            ) t
            WHERE rn = 1
            """,
            (batch_date,),
        )

        # Customers: ostatni znany stan klienta i normalizacja pol kontaktowych.
        cur.execute(
            """
            INSERT INTO stg.customers_clean
            (customer_id, email, phone, segment, country, city, postal_code, street, batch_date)
            SELECT
                customer_id,
                NULLIF(lower(trim(email)), ''),
                NULLIF(trim(phone), ''),
                lower(trim(segment)),
                NULLIF(trim(country), ''),
                NULLIF(trim(city), ''),
                NULLIF(trim(postal_code), ''),
                NULLIF(trim(street), ''),
                batch_date
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COALESCE(source_updated_at, ingested_at) DESC) AS rn
                FROM raw.customers
                WHERE batch_date = %s::date
                  AND customer_id IS NOT NULL
            ) t
            WHERE rn = 1
            """,
            (batch_date,),
        )

        # Products: ostatni znany stan SKU i casty koszt/cena/lata.
        cur.execute(
            """
            INSERT INTO stg.products_clean
            (part_sku, part_name, category, brand, manufacturer, oe_number, compatible_make, compatible_model, compatible_year_from, compatible_year_to, cost_net, price_net, batch_date)
            SELECT
                part_sku,
                part_name,
                lower(trim(category)),
                brand,
                manufacturer,
                NULLIF(trim(oe_number), ''),
                NULLIF(trim(compatible_make), ''),
                NULLIF(trim(compatible_model), ''),
                NULLIF(compatible_year_from, '')::int,
                NULLIF(compatible_year_to, '')::int,
                COALESCE(NULLIF(cost_net, '')::numeric(14,2), 0),
                COALESCE(NULLIF(price_net, '')::numeric(14,2), 0),
                batch_date
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY part_sku ORDER BY COALESCE(source_updated_at, ingested_at) DESC) AS rn
                FROM raw.products
                WHERE batch_date = %s::date
                  AND part_sku IS NOT NULL
            ) t
            WHERE rn = 1
            """,
            (batch_date,),
        )

    conn.commit()


def load_dimensions(conn) -> None:
    # Moment DWH dimensions: zasilenie i aktualizacja wymiarow.
    with conn.cursor() as cur:
        # Kalendarz na bazie dat zamowien.
        cur.execute(
            """
            INSERT INTO dwh.dim_date (date_day, year, quarter, month, day, week, day_of_week)
            SELECT DISTINCT
                o.order_date,
                EXTRACT(YEAR FROM o.order_date)::int,
                EXTRACT(QUARTER FROM o.order_date)::int,
                EXTRACT(MONTH FROM o.order_date)::int,
                EXTRACT(DAY FROM o.order_date)::int,
                EXTRACT(WEEK FROM o.order_date)::int,
                EXTRACT(ISODOW FROM o.order_date)::int
            FROM stg.orders_clean o
            ON CONFLICT (date_day) DO NOTHING
            """
        )

        # Kanaly sprzedazy.
        cur.execute(
            """
            INSERT INTO dwh.dim_channel (channel_name)
            SELECT DISTINCT sales_channel
            FROM stg.orders_clean
            WHERE sales_channel IS NOT NULL
            ON CONFLICT (channel_name) DO NOTHING
            """
        )

        # Geografia klienta.
        cur.execute(
            """
            INSERT INTO dwh.dim_geography (country, city, postal_code)
            SELECT DISTINCT country, city, postal_code
            FROM stg.customers_clean
            WHERE country IS NOT NULL OR city IS NOT NULL
            ON CONFLICT (country, city, postal_code) DO NOTHING
            """
        )

        # Customer dimension typu upsert po naturalnym kluczu customer_nk.
        cur.execute(
            """
            INSERT INTO dwh.dim_customer (customer_nk, email, phone, segment, geography_id)
            SELECT
                c.customer_id,
                c.email,
                c.phone,
                c.segment,
                g.geography_id
            FROM (
                SELECT DISTINCT ON (customer_id)
                    customer_id, email, phone, segment, country, city, postal_code, loaded_at, batch_date
                FROM stg.customers_clean
                ORDER BY customer_id, loaded_at DESC, batch_date DESC
            ) c
            LEFT JOIN dwh.dim_geography g
                ON g.country IS NOT DISTINCT FROM c.country
               AND g.city IS NOT DISTINCT FROM c.city
               AND g.postal_code IS NOT DISTINCT FROM c.postal_code
            ON CONFLICT (customer_nk) DO UPDATE
                SET email = EXCLUDED.email,
                    phone = EXCLUDED.phone,
                    segment = EXCLUDED.segment,
                    geography_id = EXCLUDED.geography_id,
                    updated_at = now()
            """
        )

        # Product dimension typu upsert po naturalnym kluczu part_sku.
        cur.execute(
            """
            INSERT INTO dwh.dim_product
            (part_sku, part_name, category, brand, manufacturer, oe_number, compatible_make, compatible_model, compatible_year_from, compatible_year_to, cost_net, price_net)
            SELECT
                p.part_sku,
                p.part_name,
                p.category,
                p.brand,
                p.manufacturer,
                p.oe_number,
                p.compatible_make,
                p.compatible_model,
                p.compatible_year_from,
                p.compatible_year_to,
                p.cost_net,
                p.price_net
            FROM (
                SELECT DISTINCT ON (part_sku)
                    part_sku, part_name, category, brand, manufacturer, oe_number,
                    compatible_make, compatible_model, compatible_year_from, compatible_year_to,
                    cost_net, price_net, loaded_at, batch_date
                FROM stg.products_clean
                ORDER BY part_sku, loaded_at DESC, batch_date DESC
            ) p
            ON CONFLICT (part_sku) DO UPDATE
                SET part_name = EXCLUDED.part_name,
                    category = EXCLUDED.category,
                    brand = EXCLUDED.brand,
                    manufacturer = EXCLUDED.manufacturer,
                    oe_number = EXCLUDED.oe_number,
                    compatible_make = EXCLUDED.compatible_make,
                    compatible_model = EXCLUDED.compatible_model,
                    compatible_year_from = EXCLUDED.compatible_year_from,
                    compatible_year_to = EXCLUDED.compatible_year_to,
                    cost_net = EXCLUDED.cost_net,
                    price_net = EXCLUDED.price_net,
                    updated_at = now()
            """
        )

    conn.commit()


def load_facts(conn, batch_date: str) -> None:
    # Moment DWH facts: zasilenie faktów zamówień i pozycji.
    with conn.cursor() as cur:
        # Fakt naglowkow zamowien.
        cur.execute(
            """
            INSERT INTO dwh.fact_orders
            (order_nk, customer_id, date_id, channel_id, order_status, currency, delivery_method, discount_amount)
            SELECT
                o.order_id,
                c.customer_id,
                d.date_id,
                ch.channel_id,
                o.status,
                o.currency,
                o.delivery_method,
                o.discount_amount
            FROM stg.orders_clean o
            JOIN dwh.dim_customer c ON c.customer_nk = o.customer_id
            JOIN dwh.dim_date d ON d.date_day = o.order_date
            LEFT JOIN dwh.dim_channel ch ON ch.channel_name = o.sales_channel
            WHERE o.batch_date = %s::date
            ON CONFLICT (order_nk) DO UPDATE
                SET customer_id = EXCLUDED.customer_id,
                    date_id = EXCLUDED.date_id,
                    channel_id = EXCLUDED.channel_id,
                    order_status = EXCLUDED.order_status,
                    currency = EXCLUDED.currency,
                    delivery_method = EXCLUDED.delivery_method,
                    discount_amount = EXCLUDED.discount_amount,
                    updated_at = now()
            """,
            (batch_date,),
        )

        # Fakt pozycji zamowien + kalkulacje kwot i marzy.
        cur.execute(
            """
            INSERT INTO dwh.fact_order_items
            (order_item_nk, order_id, product_id, qty, unit_price_net, unit_price_gross, discount_amount, tax_amount, gross_amount, net_amount, margin_amount)
            SELECT
                oi.order_item_id,
                fo.order_id,
                p.product_id,
                oi.qty,
                oi.unit_price_net,
                oi.unit_price_gross,
                oi.discount_amount,
                oi.tax_amount,
                oi.qty * oi.unit_price_gross,
                oi.qty * oi.unit_price_net,
                (oi.qty * oi.unit_price_net) - (oi.qty * p.cost_net)
            FROM stg.order_items_clean oi
            JOIN dwh.fact_orders fo ON fo.order_nk = oi.order_id
            JOIN dwh.dim_product p ON p.part_sku = oi.part_sku
            WHERE oi.batch_date = %s::date
            ON CONFLICT (order_item_nk) DO UPDATE
                SET order_id = EXCLUDED.order_id,
                    product_id = EXCLUDED.product_id,
                    qty = EXCLUDED.qty,
                    unit_price_net = EXCLUDED.unit_price_net,
                    unit_price_gross = EXCLUDED.unit_price_gross,
                    discount_amount = EXCLUDED.discount_amount,
                    tax_amount = EXCLUDED.tax_amount,
                    gross_amount = EXCLUDED.gross_amount,
                    net_amount = EXCLUDED.net_amount,
                    margin_amount = EXCLUDED.margin_amount,
                    updated_at = now()
            """,
            (batch_date,),
        )

    conn.commit()


def publish_marts(conn, views_sql_path: Path) -> None:
    # Publikacja warstwy raportowej (widoki marts).
    run_sql_file(conn, views_sql_path)
