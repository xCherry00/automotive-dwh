-- Moment 4 (MARTS): widoki biznesowe do raportowania.

-- Dzienna sprzedaz: wolumen zamowien i przychod.
CREATE OR REPLACE VIEW marts.daily_sales AS
SELECT
    dd.date_day,
    COUNT(DISTINCT fo.order_id) AS orders_count,
    SUM(foi.net_amount) AS revenue_net,
    SUM(foi.gross_amount) AS revenue_gross,
    AVG(foi.net_amount) FILTER (WHERE foi.net_amount > 0) AS avg_line_value
FROM dwh.fact_order_items foi
JOIN dwh.fact_orders fo ON fo.order_id = foi.order_id
JOIN dwh.dim_date dd ON dd.date_id = fo.date_id
GROUP BY dd.date_day;

-- Top czesci po przychodzie netto.
CREATE OR REPLACE VIEW marts.top_parts AS
SELECT
    dp.part_sku,
    dp.part_name,
    dp.category,
    dp.brand,
    SUM(foi.qty) AS units_sold,
    SUM(foi.net_amount) AS revenue_net
FROM dwh.fact_order_items foi
JOIN dwh.dim_product dp ON dp.product_id = foi.product_id
GROUP BY dp.part_sku, dp.part_name, dp.category, dp.brand
ORDER BY revenue_net DESC;

-- Marza per kategoria produktu.
CREATE OR REPLACE VIEW marts.margin_by_category AS
SELECT
    dp.category,
    SUM(foi.net_amount) AS net_sales,
    SUM(foi.margin_amount) AS margin_value,
    CASE WHEN SUM(foi.net_amount) = 0 THEN 0
         ELSE ROUND(100.0 * SUM(foi.margin_amount) / SUM(foi.net_amount), 2)
    END AS margin_pct
FROM dwh.fact_order_items foi
JOIN dwh.dim_product dp ON dp.product_id = foi.product_id
GROUP BY dp.category;

-- Retencja kohortowa: aktywnosc klientow wzgledem miesiaca pierwszego zakupu.
CREATE OR REPLACE VIEW marts.cohort_retention AS
WITH first_orders AS (
    SELECT
        fo.customer_id,
        MIN(dd.date_day) AS first_order_date
    FROM dwh.fact_orders fo
    JOIN dwh.dim_date dd ON dd.date_id = fo.date_id
    GROUP BY fo.customer_id
),
activity AS (
    SELECT
        fo.customer_id,
        dd.date_day AS order_date
    FROM dwh.fact_orders fo
    JOIN dwh.dim_date dd ON dd.date_id = fo.date_id
)
SELECT
    DATE_TRUNC('month', f.first_order_date)::date AS cohort_month,
    DATE_PART('month', AGE(DATE_TRUNC('month', a.order_date), DATE_TRUNC('month', f.first_order_date)))::int AS month_offset,
    COUNT(DISTINCT a.customer_id) AS active_customers
FROM first_orders f
JOIN activity a ON a.customer_id = f.customer_id
GROUP BY 1, 2;

-- Segmentacja RFM (recency-frequency-monetary) dla klientow.
CREATE OR REPLACE VIEW marts.rfm_segments AS
WITH base AS (
    SELECT
        c.customer_id,
        c.customer_nk,
        c.segment,
        MAX(dd.date_day) AS last_order_date,
        COUNT(DISTINCT fo.order_id) AS frequency,
        SUM(foi.net_amount) AS monetary
    FROM dwh.dim_customer c
    LEFT JOIN dwh.fact_orders fo ON fo.customer_id = c.customer_id
    LEFT JOIN dwh.dim_date dd ON dd.date_id = fo.date_id
    LEFT JOIN dwh.fact_order_items foi ON foi.order_id = fo.order_id
    GROUP BY c.customer_id, c.customer_nk, c.segment
)
SELECT
    customer_id,
    customer_nk,
    segment,
    (CURRENT_DATE - COALESCE(last_order_date, CURRENT_DATE))::int AS recency_days,
    COALESCE(frequency, 0) AS frequency,
    COALESCE(monetary, 0) AS monetary,
    CASE
        WHEN COALESCE(frequency, 0) >= 10 AND COALESCE(monetary, 0) >= 5000 THEN 'champions'
        WHEN COALESCE(frequency, 0) >= 5 AND COALESCE(monetary, 0) >= 2000 THEN 'loyal'
        WHEN COALESCE(frequency, 0) <= 1 AND (CURRENT_DATE - COALESCE(last_order_date, CURRENT_DATE)) > 90 THEN 'at_risk'
        ELSE 'regular'
    END AS rfm_segment
FROM base;
