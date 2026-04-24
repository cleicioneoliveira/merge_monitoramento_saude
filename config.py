from __future__ import annotations

from pathlib import Path
from typing import Sequence

from pydantic import BaseModel, ConfigDict


class MergeConfig(BaseModel):
    """Central configuration for merging monitoring and health timeline datasets."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    monitoramento_path: Path = Path("monitoramento_corrigido.csv")
    saude_path: Path = Path("saude_timeline_final.parquet")
    output_dir: Path = Path("outputs/merge_monitoramento_saude")

    monitoramento_datetime_col: str = "data_hora"
    saude_datetime_col: str = "timestamp"
    id_col: str = "brinco"
    saude_status_col: str = "status_vigente"
    output_status_col: str = "status_saude"

    final_columns: Sequence[str] = (
        "data_hora",
        "brinco",
        "status_saude",
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
    )

    @property
    def merged_csv_path(self) -> Path:
        return self.output_dir / "monitoramento_saude_unificado.csv"

    @property
    def merged_parquet_path(self) -> Path:
        return self.output_dir / "monitoramento_saude_unificado.parquet"

    @property
    def split_output_dir(self) -> Path:
        return self.output_dir / "por_status_saude"

    @property
    def sem_status_csv_path(self) -> Path:
        return self.output_dir / "monitoramento_sem_status_saude.csv"

    @property
    def sem_status_parquet_path(self) -> Path:
        return self.output_dir / "monitoramento_sem_status_saude.parquet"

    def ensure_dirs(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.split_output_dir.mkdir(parents=True, exist_ok=True)
