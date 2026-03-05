-- DQ-01: Brak nulli w kluczach faktów
SELECT 'fact_orders_null_fk' AS check_name, COUNT(*) AS bad_rows
FROM dwh.fact_orders
WHERE customer_id IS NULL OR date_id IS NULL;

-- DQ-02: Każda pozycja ma produkt i zamówienie
SELECT 'fact_order_items_missing_fk' AS check_name, COUNT(*) AS bad_rows
FROM dwh.fact_order_items foi
LEFT JOIN dwh.fact_orders fo ON fo.order_id = foi.order_id
LEFT JOIN dwh.dim_product dp ON dp.product_id = foi.product_id
WHERE fo.order_id IS NULL OR dp.product_id IS NULL;

-- DQ-03: Duplikaty order_nk
SELECT 'fact_orders_duplicates' AS check_name, COUNT(*) AS bad_rows
FROM (
  SELECT order_nk
  FROM dwh.fact_orders
  GROUP BY order_nk
  HAVING COUNT(*) > 1
) d;

-- DQ-04: Kontrola sum (poziom order)
SELECT 'order_sum_mismatch' AS check_name, COUNT(*) AS bad_rows
FROM (
    SELECT
        fo.order_id,
        COALESCE(fo.discount_amount, 0) AS order_discount,
        SUM(COALESCE(foi.discount_amount, 0)) AS items_discount
    FROM dwh.fact_orders fo
    JOIN dwh.fact_order_items foi ON foi.order_id = fo.order_id
    GROUP BY fo.order_id, fo.discount_amount
) x
WHERE ABS(order_discount - items_discount) > 0.01;
