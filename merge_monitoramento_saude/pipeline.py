from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .columns import (
    FINAL_RECOMMENDED_COLUMNS,
    HEALTH_REQUIRED_COLUMNS,
    MONITORING_REQUIRED_COLUMNS,
    HealthColumn,
    KeyColumn,
    normalize_columns,
    require_columns,
)
from .io import read_table, write_outputs


logger = logging.getLogger(__name__)


def _prepare_monitoramento(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize and validate corrected monitoring data."""
    prepared = normalize_columns(df)
    require_columns(prepared, MONITORING_REQUIRED_COLUMNS, "monitoramento")
    prepared[KeyColumn.DATA_HORA] = pd.to_datetime(prepared[KeyColumn.DATA_HORA], errors="coerce")
    prepared = prepared.dropna(subset=[KeyColumn.ANIMAL_ID, KeyColumn.DATA_HORA])
    return prepared


def _prepare_saude(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize and validate reconstructed health timeline."""
    prepared = normalize_columns(df)
    require_columns(prepared, HEALTH_REQUIRED_COLUMNS, "saude")
    prepared[KeyColumn.DATA_HORA] = pd.to_datetime(prepared[KeyColumn.DATA_HORA], errors="coerce")
    prepared = prepared.dropna(subset=[KeyColumn.ANIMAL_ID, KeyColumn.DATA_HORA])

    if HealthColumn.STATUS_SAUDE not in prepared.columns and HealthColumn.STATUS_VIGENTE in prepared.columns:
        prepared = prepared.rename(columns={HealthColumn.STATUS_VIGENTE: HealthColumn.STATUS_SAUDE})

    return prepared


def _order_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Move recommended final columns to the front while preserving extras."""
    preferred = [column for column in FINAL_RECOMMENDED_COLUMNS if column in df.columns]
    extras = [column for column in df.columns if column not in preferred]
    return df.loc[:, preferred + extras]


def merge_monitoramento_saude(
    monitoramento: pd.DataFrame,
    saude: pd.DataFrame,
    *,
    how: str = "left",
) -> pd.DataFrame:
    """Merge corrected monitoring data with reconstructed health timeline.

    Parameters
    ----------
    monitoramento : pandas.DataFrame
        Corrected monitoring dataset.
    saude : pandas.DataFrame
        Reconstructed health-status timeline.
    how : str, optional
        Merge strategy. Defaults to ``left`` to preserve all monitoring rows.

    Returns
    -------
    pandas.DataFrame
        Integrated dataset with canonical columns.
    """
    monitoring_df = _prepare_monitoramento(monitoramento)
    health_df = _prepare_saude(saude)

    logger.info("Monitoring rows: %s", f"{len(monitoring_df):,}")
    logger.info("Health timeline rows: %s", f"{len(health_df):,}")

    merged = monitoring_df.merge(
        health_df,
        on=[KeyColumn.ANIMAL_ID, KeyColumn.DATA_HORA],
        how=how,
        suffixes=("", "_saude"),
    )

    merged = _order_columns(merged)
    logger.info("Merged rows: %s", f"{len(merged):,}")

    return merged


def run(
    monitoramento_path: str | Path,
    saude_path: str | Path,
    output_dir: str | Path,
    *,
    output_basename: str = "monitoramento_saude_unificado",
    how: str = "left",
) -> dict[str, Path]:
    """Run the complete merge workflow from files to outputs."""
    monitoramento = read_table(monitoramento_path)
    saude = read_table(saude_path)
    merged = merge_monitoramento_saude(monitoramento, saude, how=how)
    return write_outputs(merged, output_dir, output_basename)
