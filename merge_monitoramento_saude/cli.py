from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .pipeline import run


def build_parser() -> argparse.ArgumentParser:
    """Build command-line parser."""
    parser = argparse.ArgumentParser(
        description="Merge corrected monitoring data with reconstructed health timeline."
    )
    parser.add_argument("--monitoramento", required=True, type=Path, help="Corrected monitoring CSV/Parquet file.")
    parser.add_argument("--saude", required=True, type=Path, help="Health timeline CSV/Parquet file.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Output directory.")
    parser.add_argument(
        "--output-basename",
        default="monitoramento_saude_unificado",
        help="Base name for output .parquet and .csv files.",
    )
    parser.add_argument(
        "--how",
        default="left",
        choices=["left", "inner", "outer"],
        help="Merge strategy. Default preserves all monitoring rows.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level.",
    )
    return parser


def configure_logging(level: str) -> None:
    """Configure console logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="[%(levelname)s] %(message)s",
        force=True,
    )


def main() -> int:
    """CLI entrypoint."""
    parser = build_parser()
    args = parser.parse_args()
    configure_logging(args.log_level)

    outputs = run(
        monitoramento_path=args.monitoramento,
        saude_path=args.saude,
        output_dir=args.output_dir,
        output_basename=args.output_basename,
        how=args.how,
    )

    logging.info("Parquet output: %s", outputs["parquet"])
    logging.info("CSV output: %s", outputs["csv"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
