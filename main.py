from __future__ import annotations

import argparse
from pathlib import Path

from etl.extract import get_batch_paths, validate_files
from etl.load import load_raw_batch
from etl.transform import load_dimensions, load_facts, publish_marts, run_staging_transforms
from etl.utils import execute_sql, get_connection, get_logger, load_config, run_sql_file


def parse_args() -> argparse.Namespace:
    # Parametry uruchomienia pojedynczego batcha ETL.
    parser = argparse.ArgumentParser(description="Automotive DWH pipeline")
    parser.add_argument("--date", required=True, help="Batch date in YYYY-MM-DD")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    parser.add_argument("--init-db", action="store_true", help="Run DDL before ETL")
    return parser.parse_args()


def init_db(conn, project_root: Path) -> None:
    # Inicjalizacja warstw: raw -> stg -> dwh/etl/marts.
    ddl_dir = project_root / "database" / "ddl"
    run_sql_file(conn, ddl_dir / "01_raw_schema.sql")
    run_sql_file(conn, ddl_dir / "02_stg_schema.sql")
    run_sql_file(conn, ddl_dir / "03_dwh_schema.sql")


def create_run_log(conn, run_date: str) -> int:
    # Start wpisu audytowego uruchomienia.
    row = execute_sql(
        conn,
        """
        INSERT INTO etl.etl_run_log (run_date, started_at, status)
        VALUES (%s, now(), 'RUNNING')
        RETURNING run_id
        """,
        (run_date,),
        fetch=True,
    )
    return row[0][0]


def close_run_log(conn, run_id: int, status: str, rows_loaded: int = 0, error_message: str | None = None) -> None:
    # Domkniecie logu runa finalnym statusem.
    execute_sql(
        conn,
        """
        UPDATE etl.etl_run_log
        SET ended_at = now(), status = %s, rows_loaded = %s, error_message = %s
        WHERE run_id = %s
        """,
        (status, rows_loaded, error_message, run_id),
    )


def main() -> None:
    # Orkiestracja pelnego przeplywu: ingest -> clean -> model -> marts.
    args = parse_args()
    logger = get_logger()
    cfg = load_config(Path(args.config))
    project_root = Path(__file__).resolve().parent

    conn = get_connection(cfg)
    run_id = None
    total_rows = 0

    try:
        if args.init_db:
            logger.info("Running DDL init")
            init_db(conn, project_root)

        # Rejestracja startu runa.
        run_id = create_run_log(conn, args.date)

        # Walidacja obecnosci plikow wejsciowych dla danej daty.
        incoming_root = project_root / cfg["paths"]["incoming_root"]
        batch_paths = get_batch_paths(incoming_root, args.date)
        validate_files(batch_paths)

        # Zaladowanie danych i transformacje warstwowe.
        total_rows += load_raw_batch(conn, batch_paths, args.date, logger)
        run_staging_transforms(conn, args.date)
        load_dimensions(conn)
        load_facts(conn, args.date)
        publish_marts(conn, project_root / "database" / "views" / "reporting_layer.sql")

        # Zamkniecie runa sukcesem.
        close_run_log(conn, run_id, "SUCCESS", total_rows)
        logger.info("Pipeline finished successfully. rows_loaded=%s", total_rows)
    except Exception as exc:
        # Sciezka bledu: rollback i zapis bledu do logu ETL.
        conn.rollback()
        if run_id is not None:
            close_run_log(conn, run_id, "FAILED", total_rows, str(exc)[:1000])
        logger.exception("Pipeline failed: %s", exc)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
