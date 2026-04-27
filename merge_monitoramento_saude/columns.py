from __future__ import annotations

from enum import StrEnum
import unicodedata

import pandas as pd


class KeyColumn(StrEnum):
    """Canonical key columns used to merge monitoring and health timelines."""

    ANIMAL_ID = "brinco"
    DATA_HORA = "data_hora"


class MonitoringColumn(StrEnum):
    """Canonical monitoring columns expected after environmental correction."""

    ANIMAL_ID = "brinco"
    DATA_HORA = "data_hora"
    RUMINACAO_HORA = "ruminacao_hora"
    ATIVIDADE_HORA = "atividade_hora"
    OCIO_HORA = "ocio_hora"
    OFEGACAO_HORA = "ofegacao_hora"
    RUMINACAO_ACUMULADO = "ruminacao_acumulado"
    ATIVIDADE_ACUMULADO = "atividade_acumulado"
    OCIO_ACUMULADO = "ocio_acumulado"
    OFEGACAO_ACUMULADO = "ofegacao_acumulado"
    TEMPERATURA_COMPOST_1 = "temperatura_compost_1"
    HUMIDADE_COMPOST_1 = "humidade_compost_1"
    THI_COMPOST_1 = "thi_compost1"
    TEMPERATURA_COMPOST_2 = "temperatura_compost_2"
    HUMIDADE_COMPOST_2 = "humidade_compost_2"
    THI_COMPOST_2 = "thi_compost2"


class HealthColumn(StrEnum):
    """Canonical health-timeline columns."""

    ANIMAL_ID = "brinco"
    DATA_HORA = "data_hora"
    STATUS_VIGENTE = "status_vigente"
    STATUS_SAUDE = "status_saude"
    STATUS_INICIO_VIGENCIA = "status_inicio_vigencia"
    STATUS_FIM_VIGENCIA_INFERIDO = "status_fim_vigencia_inferido"
    PROXIMA_MUDANCA = "proxima_mudanca"
    EPISODE_NUMBER = "episode_number"


class FinalColumn(StrEnum):
    """Recommended stable columns exported to the thermal-comfort package."""

    ANIMAL_ID = "brinco"
    DATA_HORA = "data_hora"
    STATUS_SAUDE = "status_saude"
    RUMINACAO_HORA = "ruminacao_hora"
    ATIVIDADE_HORA = "atividade_hora"
    OCIO_HORA = "ocio_hora"
    OFEGACAO_HORA = "ofegacao_hora"
    RUMINACAO_ACUMULADO = "ruminacao_acumulado"
    ATIVIDADE_ACUMULADO = "atividade_acumulado"
    OCIO_ACUMULADO = "ocio_acumulado"
    OFEGACAO_ACUMULADO = "ofegacao_acumulado"
    TEMPERATURA_COMPOST_1 = "temperatura_compost_1"
    HUMIDADE_COMPOST_1 = "humidade_compost_1"
    THI_COMPOST_1 = "thi_compost1"
    TEMPERATURA_COMPOST_2 = "temperatura_compost_2"
    HUMIDADE_COMPOST_2 = "humidade_compost_2"
    THI_COMPOST_2 = "thi_compost2"


COLUMN_ALIASES: dict[str, str] = {
    "animal_id": KeyColumn.ANIMAL_ID,
    "id_animal": KeyColumn.ANIMAL_ID,
    "brinco": KeyColumn.ANIMAL_ID,
    "timestamp": KeyColumn.DATA_HORA,
    "datetime": KeyColumn.DATA_HORA,
    "data": KeyColumn.DATA_HORA,
    "data_hora": KeyColumn.DATA_HORA,
    "status": FinalColumn.STATUS_SAUDE,
    "status_atual": FinalColumn.STATUS_SAUDE,
    "status_vigente": HealthColumn.STATUS_VIGENTE,
    "status_saude": FinalColumn.STATUS_SAUDE,
    "temperatura_compost1": MonitoringColumn.TEMPERATURA_COMPOST_1,
    "temperatura_compost_1": MonitoringColumn.TEMPERATURA_COMPOST_1,
    "humidade_compost1": MonitoringColumn.HUMIDADE_COMPOST_1,
    "humidade_compost_1": MonitoringColumn.HUMIDADE_COMPOST_1,
    "umidade_compost_1": MonitoringColumn.HUMIDADE_COMPOST_1,
    "thi_compost_1": MonitoringColumn.THI_COMPOST_1,
    "thi_compost1": MonitoringColumn.THI_COMPOST_1,
    "temperatura_compost2": MonitoringColumn.TEMPERATURA_COMPOST_2,
    "temperatura_compost_2": MonitoringColumn.TEMPERATURA_COMPOST_2,
    "humidade_compost2": MonitoringColumn.HUMIDADE_COMPOST_2,
    "humidade_compost_2": MonitoringColumn.HUMIDADE_COMPOST_2,
    "umidade_compost_2": MonitoringColumn.HUMIDADE_COMPOST_2,
    "thi_compost_2": MonitoringColumn.THI_COMPOST_2,
    "thi_compost2": MonitoringColumn.THI_COMPOST_2,
}

KEY_COLUMNS: tuple[str, str] = (
    KeyColumn.ANIMAL_ID.value,
    KeyColumn.DATA_HORA.value,
)

MONITORING_REQUIRED_COLUMNS: tuple[str, ...] = KEY_COLUMNS
HEALTH_REQUIRED_COLUMNS: tuple[str, ...] = (
    KeyColumn.ANIMAL_ID.value,
    KeyColumn.DATA_HORA.value,
)

FINAL_RECOMMENDED_COLUMNS: tuple[str, ...] = tuple(item.value for item in FinalColumn)


def normalize_col(name: str) -> str:
    """Normalize a column name to ASCII snake_case."""
    value = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii")
    return (
        value.strip()
        .lower()
        .replace(" ", "_")
        .replace(".", "")
        .replace("/", "_")
        .replace("-", "_")
    )


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame columns and apply known aliases."""
    normalized = df.copy()
    normalized.columns = [normalize_col(column) for column in normalized.columns]
    rename_map = {
        column: COLUMN_ALIASES[column]
        for column in normalized.columns
        if column in COLUMN_ALIASES and COLUMN_ALIASES[column] not in normalized.columns
    }
    return normalized.rename(columns=rename_map) if rename_map else normalized


def require_columns(df: pd.DataFrame, required: tuple[str, ...], df_name: str) -> None:
    """Validate required columns."""
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"Colunas obrigatórias ausentes em {df_name}: {missing}")
