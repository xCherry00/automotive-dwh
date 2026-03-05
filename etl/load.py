from __future__ import annotations

import csv
from pathlib import Path

from psycopg2 import sql

FILE_TO_TABLE = {
    "customers.csv": "raw.customers",
    "products.csv": "raw.products",
    "orders.csv": "raw.orders",
    "order_items.csv": "raw.order_items",
}


def _get_csv_headers(path: Path) -> list[str]:
    # Odczyt naglowkow CSV, aby dynamicznie przygotowac COPY.
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        return next(reader)


def copy_csv_to_raw(conn, csv_path: Path, target_table: str, batch_date: str) -> int:
    # Moment load do raw: COPY -> cast minimalny -> insert z metadanymi batcha.
    headers = _get_csv_headers(csv_path)
    temp_table = "tmp_load"

    with conn.cursor() as cur:
        # Etap 1: tymczasowa tabela tekstowa zgodna z CSV.
        col_defs = sql.SQL(", ").join(sql.SQL("{} TEXT").format(sql.Identifier(h)) for h in headers)
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {};").format(sql.Identifier(temp_table)))
        cur.execute(sql.SQL("CREATE TEMP TABLE {} ({}) ON COMMIT DROP;").format(sql.Identifier(temp_table), col_defs))

        # Etap 2: szybki import pliku do temp.
        with csv_path.open("r", encoding="utf-8") as f:
            copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH CSV HEADER").format(
                sql.Identifier(temp_table),
                sql.SQL(", ").join(sql.Identifier(h) for h in headers),
            )
            cur.copy_expert(copy_sql.as_string(conn), f)

        select_exprs = []
        for h in headers:
            # source_updated_at wymaga jawnego castu do TIMESTAMP.
            if h == "source_updated_at":
                select_exprs.append(
                    sql.SQL("NULLIF({}, '')::timestamp").format(sql.Identifier(h))
                )
            else:
                select_exprs.append(sql.Identifier(h))

        # Etap 3: insert do raw + metadane techniczne.
        insert_sql = sql.SQL(
            """
            INSERT INTO {} ({}, ingested_at, batch_date)
            SELECT {}, now(), %s::date
            FROM {}
            """
        ).format(
            sql.SQL(target_table),
            sql.SQL(", ").join(sql.Identifier(h) for h in headers),
            sql.SQL(", ").join(select_exprs),
            sql.Identifier(temp_table),
        )
        cur.execute(insert_sql, (batch_date,))
        rowcount = cur.rowcount

    conn.commit()
    return rowcount


def load_raw_batch(conn, batch_paths: dict[str, Path], batch_date: str, logger) -> int:
    # Laduje komplet plikow wejsciowych do warstwy raw.
    total_rows = 0
    for filename, table_name in FILE_TO_TABLE.items():
        csv_path = batch_paths[filename]
        rows = copy_csv_to_raw(conn, csv_path, table_name, batch_date)
        total_rows += rows
        logger.info("Loaded %s rows into %s", rows, table_name)
    return total_rows
