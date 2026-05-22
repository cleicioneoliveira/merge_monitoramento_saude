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


def _fill_missing_health_status(df: pd.DataFrame, default_status: str = "Normal") -> pd.DataFrame:
    """Fill health-status gaps left after the monitoring-health merge.

    A missing status after the left merge means that the animal-hour record did
    not find a corresponding reconstructed health-status record. In this
    project, the basal state for animals without an active or reconstructed
    health event is considered ``Normal``.
    """
    filled = df.copy()
    if HealthColumn.STATUS_SAUDE not in filled.columns:
        return filled

    missing = filled[HealthColumn.STATUS_SAUDE].isna()
    n_missing = int(missing.sum())
    if n_missing:
        n_animals = filled.loc[missing, KeyColumn.ANIMAL_ID].nunique()
        logger.info(
            "Health status missing after merge: filling %s rows from %s animals as %s.",
            f"{n_missing:,}",
            f"{n_animals:,}",
            default_status,
        )
        filled.loc[missing, HealthColumn.STATUS_SAUDE] = default_status
    else:
        logger.info("No missing health status after merge.")

    return filled


def _select_final_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return only the final canonical dataset columns, in the official order."""
    missing = [column for column in FINAL_RECOMMENDED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(
            "Colunas finais obrigatórias ausentes após o merge: "
            f"{missing}. Colunas disponíveis: {list(df.columns)}"
        )

    return df.loc[:, list(FINAL_RECOMMENDED_COLUMNS)].copy()


def merge_monitoramento_saude(
    monitoramento: pd.DataFrame,
    saude: pd.DataFrame,
    *,
    how: str = "left",
    fill_missing_status: bool = True,
    default_status: str = "Normal",
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
    fill_missing_status : bool, optional
        If True, missing health status after the merge is filled with
        ``default_status``.
    default_status : str, optional
        Basal status assigned to animal-hour records without a reconstructed
        health event. Defaults to ``Normal``.

    Returns
    -------
    pandas.DataFrame
        Integrated dataset restricted to the final canonical columns.
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

    if fill_missing_status:
        merged = _fill_missing_health_status(merged, default_status=default_status)

    merged = _select_final_columns(merged)
    logger.info("Merged rows: %s", f"{len(merged):,}")
    logger.info("Final columns: %s", list(merged.columns))

    return merged


def run(
    monitoramento_path: str | Path,
    saude_path: str | Path,
    output_dir: str | Path,
    *,
    output_basename: str = "monitoramento_saude_unificado",
    how: str = "left",
    fill_missing_status: bool = True,
    default_status: str = "Normal",
) -> dict[str, Path]:
    """Run the complete merge workflow from files to outputs."""
    monitoramento = read_table(monitoramento_path)
    saude = read_table(saude_path)
    merged = merge_monitoramento_saude(
        monitoramento,
        saude,
        how=how,
        fill_missing_status=fill_missing_status,
        default_status=default_status,
    )
    return write_outputs(merged, output_dir, output_basename)
