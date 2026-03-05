from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import psycopg2
import yaml


def get_logger() -> logging.Logger:
    # Wspolny logger dla calego pipeline.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger("automotive-dwh")


def load_config(path: Path) -> dict[str, Any]:
    # Wczytanie konfiguracji srodowiskowej z YAML.
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_connection(config: dict[str, Any]):
    # Tworzy polaczenie do PostgreSQL na podstawie config.yaml.
    db_cfg = config["database"]
    return psycopg2.connect(
        host=db_cfg["host"],
        port=db_cfg["port"],
        dbname=db_cfg["dbname"],
        user=db_cfg["user"],
        password=db_cfg["password"],
    )


def execute_sql(conn, sql_text: str, params: tuple | None = None, fetch: bool = False):
    # Helper transakcyjny: execute + opcjonalny fetch + commit/rollback.
    try:
        with conn.cursor() as cur:
            cur.execute(sql_text, params)
            rows = cur.fetchall() if fetch else None
        conn.commit()
        return rows
    except Exception:
        conn.rollback()
        raise


def run_sql_file(conn, path: Path) -> None:
    # Uruchomienie calego pliku SQL (DDL lub widoki).
    sql_text = path.read_text(encoding="utf-8-sig")
    with conn.cursor() as cur:
        cur.execute(sql_text)
    conn.commit()
