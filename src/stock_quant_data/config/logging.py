"""
Logging configuration for stock-quant-data-loader.

Intent:
- deterministic logging
- clean CLI logs
- no duplicate handlers on repeated imports
"""

from __future__ import annotations

import logging
import sys


def configure_logging(level: int = logging.INFO) -> None:
    """
    Configure root logging exactly once.

    Why force=True:
    - many of the jobs are executed repeatedly in the same interpreter during
      tests or orchestration
    - force=True prevents duplicate handlers and duplicated log lines
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        stream=sys.stdout,
        force=True,
    )
