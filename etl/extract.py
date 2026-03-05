from __future__ import annotations

from pathlib import Path

EXPECTED_FILES = ("customers.csv", "products.csv", "orders.csv", "order_items.csv")


def get_batch_paths(incoming_root: Path, batch_date: str) -> dict[str, Path]:
    # Buduje mape wymaganych plikow dla konkretnej daty batcha.
    batch_dir = incoming_root / batch_date
    return {name: batch_dir / name for name in EXPECTED_FILES}


def validate_files(batch_paths: dict[str, Path]) -> None:
    # Szybka walidacja wejscia przed loadem do bazy.
    missing = [name for name, path in batch_paths.items() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing files in batch: {', '.join(missing)}")
