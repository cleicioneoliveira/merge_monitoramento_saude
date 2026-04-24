from __future__ import annotations

import pandas as pd

from .config import MergeConfig
from .io_utils import (
    log_info,
    normalize_status_for_filename,
    read_table,
    validate_required_columns,
)


def prepare_monitoramento(df: pd.DataFrame, cfg: MergeConfig) -> pd.DataFrame:
    """Prepare monitoring table."""
    log_info("Preparing monitoring dataset.")
    df = df.copy()

    df[cfg.monitoramento_datetime_col] = pd.to_datetime(
        df[cfg.monitoramento_datetime_col],
        errors="coerce",
    )

    if df[cfg.monitoramento_datetime_col].isna().any():
        n_invalid = int(df[cfg.monitoramento_datetime_col].isna().sum())
        log_info(f"Monitoring dataset has {n_invalid} invalid datetime row(s).")

    return df


def prepare_saude(df: pd.DataFrame, cfg: MergeConfig) -> pd.DataFrame:
    """Prepare health timeline table."""
    log_info("Preparing health timeline dataset.")
    df = df.copy()

    df[cfg.saude_datetime_col] = pd.to_datetime(
        df[cfg.saude_datetime_col],
        errors="coerce",
    )

    if df[cfg.saude_datetime_col].isna().any():
        n_invalid = int(df[cfg.saude_datetime_col].isna().sum())
        log_info(f"Health dataset has {n_invalid} invalid datetime row(s).")

    return df


def build_merged_dataset(
    monitoramento: pd.DataFrame,
    saude: pd.DataFrame,
    cfg: MergeConfig,
) -> pd.DataFrame:
    """Merge monitoring and health timeline datasets."""
    log_info("Selecting only required health columns for merge.")
    saude_merge = saude[[cfg.id_col, cfg.saude_datetime_col, cfg.saude_status_col]].copy()

    log_info(
        f"Merging datasets on [{cfg.id_col}] and "
        f"[{cfg.monitoramento_datetime_col} == {cfg.saude_datetime_col}]."
    )
    merged = monitoramento.merge(
        saude_merge,
        how="left",
        left_on=[cfg.id_col, cfg.monitoramento_datetime_col],
        right_on=[cfg.id_col, cfg.saude_datetime_col],
        validate="many_to_one",
    )

    log_info("Renaming health status column to final output name.")
    merged = merged.rename(columns={cfg.saude_status_col: cfg.output_status_col})

    if cfg.saude_datetime_col in merged.columns:
        merged = merged.drop(columns=[cfg.saude_datetime_col])

    log_info("Selecting final output columns.")
    merged = merged.loc[:, list(cfg.final_columns)].copy()

    merged = merged.sort_values(
        by=[cfg.monitoramento_datetime_col, cfg.id_col],
        kind="mergesort",
    ).reset_index(drop=True)

    return merged


def save_split_by_status(df: pd.DataFrame, cfg: MergeConfig) -> None:
    """Save one file per health status."""
    log_info("Saving files split by health status.")

    if cfg.output_status_col not in df.columns:
        raise ValueError(f"Column '{cfg.output_status_col}' not found in merged dataset.")

    with_status = df[df[cfg.output_status_col].notna()].copy()
    without_status = df[df[cfg.output_status_col].isna()].copy()

    unique_statuses = sorted(with_status[cfg.output_status_col].dropna().unique())
    log_info(f"Found {len(unique_statuses)} status group(s) with data.")

    for status in unique_statuses:
        status_df = with_status[with_status[cfg.output_status_col] == status].copy()
        safe_status = normalize_status_for_filename(status)

        csv_path = cfg.split_output_dir / f"monitoramento_saude_{safe_status}.csv"
        parquet_path = cfg.split_output_dir / f"monitoramento_saude_{safe_status}.parquet"

        log_info(f"Saving status '{status}' with {len(status_df)} row(s).")
        status_df.to_csv(csv_path, index=False)
        status_df.to_parquet(parquet_path, index=False)

    if not without_status.empty:
        log_info(f"Saving rows without matched health status: {len(without_status)} row(s).")
        without_status.to_csv(cfg.sem_status_csv_path, index=False)
        without_status.to_parquet(cfg.sem_status_parquet_path, index=False)
    else:
        log_info("All monitoring rows received a matched health status.")


def run_pipeline(cfg: MergeConfig) -> pd.DataFrame:
    """Run full merge pipeline."""
    cfg.ensure_dirs()

    log_info(f"Reading monitoring file: {cfg.monitoramento_path}")
    monitoramento = read_table(cfg.monitoramento_path)
    log_info(f"Monitoring dataset loaded with {len(monitoramento)} row(s).")

    log_info(f"Reading health file: {cfg.saude_path}")
    saude = read_table(cfg.saude_path)
    log_info(f"Health dataset loaded with {len(saude)} row(s).")

    validate_required_columns(
        monitoramento,
        [
            cfg.id_col,
            cfg.monitoramento_datetime_col,
            "ruminacao_hora",
            "atividade_hora",
            "ocio_hora",
            "ofegacao_hora",
            "ruminacao_acumulado",
            "atividade_acumulado",
            "ocio_acumulado",
            "ofegacao_acumulado",
            "humidade_compost_1",
            "temperatura_compost_1",
            "thi_compost1",
            "humidade_compost_2",
            "temperatura_compost_2",
            "thi_compost2",
        ],
        name="monitoramento",
    )

    validate_required_columns(
        saude,
        [cfg.id_col, cfg.saude_datetime_col, cfg.saude_status_col],
        name="saude",
    )

    monitoramento = prepare_monitoramento(monitoramento, cfg)
    saude = prepare_saude(saude, cfg)

    merged = build_merged_dataset(monitoramento, saude, cfg)

    n_total = len(merged)
    n_with_status = int(merged[cfg.output_status_col].notna().sum())
    n_without_status = int(merged[cfg.output_status_col].isna().sum())

    log_info(f"Merged dataset created with {n_total} row(s).")
    log_info(f"Rows with matched health status: {n_with_status}")
    log_info(f"Rows without matched health status: {n_without_status}")

    log_info("Saving unified merged dataset.")
    merged.to_csv(cfg.merged_csv_path, index=False)
    merged.to_parquet(cfg.merged_parquet_path, index=False)

    save_split_by_status(merged, cfg)

    log_info(f"Unified CSV saved to: {cfg.merged_csv_path}")
    log_info(f"Unified Parquet saved to: {cfg.merged_parquet_path}")

    return merged
