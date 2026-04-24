from __future__ import annotations

from pathlib import Path
from typing import Sequence

import pandas as pd


def log_info(message: str) -> None:
    """Print standardized informational messages."""
    print(f"[INFO] {message}")


def read_table(path: Path) -> pd.DataFrame:
    """Read input table according to file extension."""
    suffix = path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)

    raise ValueError(
        f"Unsupported input format: {suffix}. Use .csv, .parquet, .xlsx or .xls."
    )


def validate_required_columns(
    df: pd.DataFrame,
    required_columns: Sequence[str],
    name: str,
) -> None:
    """Validate that all required columns exist in a DataFrame."""
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns in '{name}': {missing}\n"
            f"Available columns in '{name}': {list(df.columns)}"
        )


def normalize_status_for_filename(value: str) -> str:
    """Make status values safe for filenames."""
    return (
        str(value)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
    )
