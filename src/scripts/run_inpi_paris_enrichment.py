from __future__ import annotations

import argparse

from src.pipelines.run_inpi_rne_enrichment import run_inpi_rne_enrichment_for_paris
from src.utils.logger import get_logger


logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich Paris establishments with INPI RNE API data using existing SIRET values."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of Paris candidate rows to process.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="If provided, reprocess rows even if they already have INPI RNE data.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    summary = run_inpi_rne_enrichment_for_paris(
        limit=args.limit,
        only_missing=not args.force,
    )

    logger.info("Summary: %s", summary)


if __name__ == "__main__":
    main()