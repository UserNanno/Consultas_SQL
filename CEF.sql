import logging
import os
import tempfile
from pathlib import Path

def pick_log_path(app_dir: Path) -> Path:
    """
    Intenta crear el log junto al exe/script.
    Si no hay permisos, usa LOCALAPPDATA o TEMP.
    """
    primary = app_dir / "prisma_selenium.log"
    try:
        primary.parent.mkdir(parents=True, exist_ok=True)
        primary.write_text("test", encoding="utf-8")
        primary.unlink(missing_ok=True)
        return primary
    except Exception:
        base = Path(os.environ.get("LOCALAPPDATA", tempfile.gettempdir()))
        fallback = base / "PrismaProject" / "prisma_selenium.log"
        fallback.parent.mkdir(parents=True, exist_ok=True)
        return fallback


def setup_logging(app_dir: Path):
    log_path = pick_log_path(app_dir)

    logging.basicConfig(
        filename=str(log_path),
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        force=True,
        encoding="utf-8",
    )

    logging.info("=== LOG INICIALIZADO ===")
    logging.info("LOG_PATH=%s", log_path.resolve())
