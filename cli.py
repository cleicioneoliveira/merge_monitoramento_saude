#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from .config import MergeConfig
from .io_utils import log_info
from .pipeline import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Merge monitoring data with reconstructed health timeline and "
            "generate one unified dataset plus files split by health status."
        )
    )

    parser.add_argument(
        "--monitoramento",
        type=Path,
        required=True,
        help="Path to monitoring dataset (.csv or .parquet).",
    )
    parser.add_argument(
        "--saude",
        type=Path,
        required=True,
        help="Path to health timeline dataset (.csv or .parquet).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory where merged outputs will be written.",
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    cfg = MergeConfig(
        monitoramento_path=args.monitoramento,
        saude_path=args.saude,
        output_dir=args.output_dir,
    )

    merged = run_pipeline(cfg)

    log_info("Merge pipeline finished successfully.")
    log_info(f"Final unified dataset rows: {len(merged)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
