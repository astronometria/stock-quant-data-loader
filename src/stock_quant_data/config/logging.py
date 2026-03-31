"""
Logging helpers.

The goal is not to build a giant logging framework in v1.
We only want:
- readable terminal logs
- consistent timestamps
- minimal structured context
"""

import logging
import sys


def configure_logging(level: int = logging.INFO) -> None:
    """
    Configure root logging once for the application.

    This is intentionally simple for v1.
    It can later evolve into JSON logs if needed.
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        stream=sys.stdout,
        force=True,
    )
