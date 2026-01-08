# utils/logging_utils.py
import logging
import tempfile
from pathlib import Path

LOG_PATH = Path(tempfile.gettempdir()) / "prisma_selenium.log"

def setup_logging():
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=str(LOG_PATH),
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        force=True,
        encoding="utf-8",
    )
