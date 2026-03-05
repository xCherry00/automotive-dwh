from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

SEGMENTS = ["detal", "serwis", "flota"]
CHANNELS = ["web", "allegro", "workshop_b2b", "mobile_app"]
DELIVERY_METHODS = ["courier", "pickup_point", "in_store"]
ORDER_STATUSES = ["new", "paid", "shipped", "returned", "cancelled"]
CATEGORIES = ["hamulce", "zawieszenie", "filtracja", "elektryka", "silnik", "uklad_chlodzenia"]
BRANDS = ["Bosch", "TRW", "Febi", "Mann", "Valeo", "Sachs", "Denso", "Brembo"]
MANUFACTURERS = ["Bosch", "ZF", "Continental", "Valeo", "Magneti Marelli", "Aisin"]
MAKES_MODELS = {
    "VW": ["Golf", "Passat", "Tiguan", "Polo"],
    "BMW": ["3", "5", "X3", "X5"],
    "Audi": ["A3", "A4", "A6", "Q5"],
    "Skoda": ["Octavia", "Fabia", "Superb"],
    "Ford": ["Focus", "Mondeo", "Kuga"],
    "Toyota": ["Corolla", "Avensis", "RAV4"],
}
CITIES = ["Warszawa", "Krakow", "Wroclaw", "Poznan", "Gdansk", "Lodz", "Katowice", "Szczecin"]
STREETS = ["Dluga", "Krotka", "Polna", "Warszawska", "Mickiewicza", "Sienkiewicza", "Kolejowa"]


def parse_args() -> argparse.Namespace:
    # Konfiguracja skali i parametrow generatora danych syntetycznych.
    parser = argparse.ArgumentParser(description="Generate large synthetic CSV batches for automotive DWH")
    parser.add_argument("--date", required=True, help="Batch date in YYYY-MM-DD")
    parser.add_argument("--customers", type=int, default=200_000, help="Number of customers")
    parser.add_argument("--products", type=int, default=100_000, help="Number of products")
    parser.add_argument("--orders", type=int, default=1_000_000, help="Number of orders")
    parser.add_argument("--min-items", type=int, default=1, help="Min items per order")
    parser.add_argument("--max-items", type=int, default=5, help="Max items per order")
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed. If omitted, a deterministic seed is derived from --date.",
    )
    parser.add_argument("--output-root", default="data/incoming", help="Root directory for output files")
    parser.add_argument(
        "--profile",
        choices=["dev", "large", "xlarge"],
        help="Optional preset scale. Overrides customers/products/orders if set.",
    )
    return parser.parse_args()


def resolve_seed(batch_date: str, seed: int | None) -> int:
    # Determinizm: ta sama data daje identyczny zestaw danych.
    if seed is not None:
        return seed
    # Stable per day: same date => same data, different date => different data.
    return int(batch_date.replace("-", ""))


def apply_profile(args: argparse.Namespace) -> None:
    # Presety wolumenu do szybkiego przelaczania miedzy srodowiskami.
    if args.profile == "dev":
        args.customers = 5_000
        args.products = 2_000
        args.orders = 20_000
    elif args.profile == "large":
        args.customers = 200_000
        args.products = 100_000
        args.orders = 1_000_000
    elif args.profile == "xlarge":
        args.customers = 1_000_000
        args.products = 250_000
        args.orders = 5_000_000


def ensure_output_dir(root: str, batch_date: str) -> Path:
    # Kazdy batch zapisujemy do osobnego katalogu daty.
    out_dir = Path(root) / batch_date
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def random_ts_for_day(batch_date: str, rng: random.Random) -> tuple[str, str]:
    # Losowe timestampy w obrebie jednego dnia batcha.
    day_start = datetime.strptime(batch_date, "%Y-%m-%d")
    order_dt = day_start + timedelta(seconds=rng.randint(0, 86_399))
    update_dt = order_dt + timedelta(minutes=rng.randint(1, 240))
    return order_dt.strftime("%Y-%m-%d %H:%M:%S"), update_dt.strftime("%Y-%m-%d %H:%M:%S")


def generate_customers(path: Path, n_customers: int, batch_date: str, rng: random.Random) -> None:
    # Moment 1: generowanie klientow (zrodlo wymiaru customer).
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "customer_id",
            "email",
            "phone",
            "segment",
            "country",
            "city",
            "postal_code",
            "street",
            "source_updated_at",
        ])

        for i in range(1, n_customers + 1):
            customer_id = f"C{i:09d}"
            city = rng.choice(CITIES)
            street = f"{rng.choice(STREETS)} {rng.randint(1, 199)}"
            postal_code = f"{rng.randint(10, 99)}-{rng.randint(100, 999)}"
            _, source_updated_at = random_ts_for_day(batch_date, rng)
            w.writerow([
                customer_id,
                f"{customer_id.lower()}@example.com",
                f"+48{rng.randint(500000000, 899999999)}",
                rng.choices(SEGMENTS, weights=[70, 20, 10], k=1)[0],
                "PL",
                city,
                postal_code,
                street,
                source_updated_at,
            ])

            if i % 200_000 == 0:
                print(f"customers: {i}")


def generate_products(path: Path, n_products: int, batch_date: str, rng: random.Random) -> None:
    # Moment 2: generowanie katalogu produktow (zrodlo wymiaru product).
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "part_sku",
            "part_name",
            "category",
            "brand",
            "manufacturer",
            "oe_number",
            "compatible_make",
            "compatible_model",
            "compatible_year_from",
            "compatible_year_to",
            "cost_net",
            "price_net",
            "source_updated_at",
        ])

        for i in range(1, n_products + 1):
            part_sku = f"SKU{i:09d}"
            category = rng.choice(CATEGORIES)
            brand = rng.choice(BRANDS)
            manufacturer = rng.choice(MANUFACTURERS)
            make = rng.choice(list(MAKES_MODELS.keys()))
            model = rng.choice(MAKES_MODELS[make])
            year_from = rng.randint(2000, 2020)
            year_to = rng.randint(max(year_from, 2005), 2026)
            cost_net = round(rng.uniform(20, 900), 2)
            markup = rng.uniform(1.15, 1.65)
            price_net = round(cost_net * markup, 2)
            _, source_updated_at = random_ts_for_day(batch_date, rng)

            w.writerow([
                part_sku,
                f"{category}_{brand}_{i}",
                category,
                brand,
                manufacturer,
                f"OE{rng.randint(1000000, 9999999)}",
                make,
                model,
                str(year_from),
                str(year_to),
                f"{cost_net:.2f}",
                f"{price_net:.2f}",
                source_updated_at,
            ])

            if i % 200_000 == 0:
                print(f"products: {i}")


def generate_orders_and_items(
    orders_path: Path,
    items_path: Path,
    n_orders: int,
    n_customers: int,
    n_products: int,
    min_items: int,
    max_items: int,
    batch_date: str,
    rng: random.Random,
) -> None:
    # Moment 3: generowanie transakcji (orders) i pozycji (order_items).
    with orders_path.open("w", newline="", encoding="utf-8") as fo, items_path.open("w", newline="", encoding="utf-8") as fi:
        wo = csv.writer(fo)
        wi = csv.writer(fi)

        wo.writerow([
            "order_id",
            "customer_id",
            "order_created_at",
            "status",
            "sales_channel",
            "delivery_method",
            "discount_amount",
            "currency",
            "source_updated_at",
        ])

        wi.writerow([
            "order_item_id",
            "order_id",
            "line_no",
            "part_sku",
            "qty",
            "unit_price_net",
            "unit_price_gross",
            "discount_amount",
            "tax_amount",
            "source_updated_at",
        ])

        item_counter = 0
        for i in range(1, n_orders + 1):
            order_id = f"O{i:010d}"
            customer_id = f"C{rng.randint(1, n_customers):09d}"
            order_created_at, source_updated_at = random_ts_for_day(batch_date, rng)
            status = rng.choices(ORDER_STATUSES, weights=[5, 35, 45, 10, 5], k=1)[0]
            channel = rng.choices(CHANNELS, weights=[45, 25, 20, 10], k=1)[0]
            delivery_method = rng.choices(DELIVERY_METHODS, weights=[70, 20, 10], k=1)[0]
            order_discount = round(rng.uniform(0, 80), 2)

            wo.writerow([
                order_id,
                customer_id,
                order_created_at,
                status,
                channel,
                delivery_method,
                f"{order_discount:.2f}",
                "PLN",
                source_updated_at,
            ])

            lines = rng.randint(min_items, max_items)
            order_discount_left = order_discount
            for line_no in range(1, lines + 1):
                item_counter += 1
                part_sku = f"SKU{rng.randint(1, n_products):09d}"
                qty = rng.randint(1, 8)
                unit_price_net = round(rng.uniform(25, 1200), 2)
                unit_price_gross = round(unit_price_net * 1.23, 2)
                line_tax = round((unit_price_gross - unit_price_net) * qty, 2)

                if line_no == lines:
                    line_discount = round(max(order_discount_left, 0), 2)
                else:
                    max_split = max(order_discount_left, 0)
                    line_discount = round(rng.uniform(0, max_split), 2)
                    order_discount_left = round(order_discount_left - line_discount, 2)

                wi.writerow([
                    f"OI{item_counter:012d}",
                    order_id,
                    str(line_no),
                    part_sku,
                    f"{qty}",
                    f"{unit_price_net:.2f}",
                    f"{unit_price_gross:.2f}",
                    f"{line_discount:.2f}",
                    f"{line_tax:.2f}",
                    source_updated_at,
                ])

            if i % 200_000 == 0:
                print(f"orders: {i}, order_items: {item_counter}")

        print(f"orders: {n_orders}, order_items: {item_counter}")


def main() -> None:
    # Orkiestracja generatora: walidacja -> seed -> eksport CSV.
    args = parse_args()
    apply_profile(args)

    if args.customers <= 0 or args.products <= 0 or args.orders <= 0:
        raise ValueError("customers/products/orders must be > 0")
    if args.min_items <= 0 or args.max_items < args.min_items:
        raise ValueError("Invalid item range")

    seed = resolve_seed(args.date, args.seed)
    rng = random.Random(seed)
    out_dir = ensure_output_dir(args.output_root, args.date)

    customers_path = out_dir / "customers.csv"
    products_path = out_dir / "products.csv"
    orders_path = out_dir / "orders.csv"
    order_items_path = out_dir / "order_items.csv"

    print(f"Using seed: {seed}")
    print("Generating customers...")
    generate_customers(customers_path, args.customers, args.date, rng)

    print("Generating products...")
    generate_products(products_path, args.products, args.date, rng)

    print("Generating orders and order_items...")
    generate_orders_and_items(
        orders_path,
        order_items_path,
        args.orders,
        args.customers,
        args.products,
        args.min_items,
        args.max_items,
        args.date,
        rng,
    )

    print("Done")
    print(f"Output directory: {out_dir}")


if __name__ == "__main__":
    main()
