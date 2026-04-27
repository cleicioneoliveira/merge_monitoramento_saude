from __future__ import annotations

from pathlib import Path

import pandas as pd


def read_table(path: str | Path) -> pd.DataFrame:
    """Read CSV, Excel or Parquet file based on extension."""
    file_path = Path(path)
    suffix = file_path.suffix.lower()

    if suffix == ".parquet":
        return pd.read_parquet(file_path)

    if suffix in {".csv", ".txt"}:
        return pd.read_csv(file_path)

    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(file_path)

    raise ValueError(f"Formato não suportado: {file_path}")


def write_outputs(df: pd.DataFrame, output_dir: str | Path, basename: str) -> dict[str, Path]:
    """Write merged dataset as Parquet and CSV."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    parquet_path = output_path / f"{basename}.parquet"
    csv_path = output_path / f"{basename}.csv"

    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)

    return {"parquet": parquet_path, "csv": csv_path}
