import logging
import tempfile
from pathlib import Path

LOG_PATH = Path(tempfile.gettempdir()) / "prisma_selenium.log"

def setup_logging():
    logging.basicConfig(
        filename=str(LOG_PATH),
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
