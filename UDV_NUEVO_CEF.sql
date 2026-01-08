import json
import os
import tempfile
from pathlib import Path
from typing import Tuple

APP_NAME = "PrismaProject"
FILE_NAME = "credentials.json"


def _base_dir() -> Path:
    base = Path(os.environ.get("LOCALAPPDATA", tempfile.gettempdir()))
    d = base / APP_NAME
    d.mkdir(parents=True, exist_ok=True)
    return d


def get_credentials_path() -> Path:
    return _base_dir() / FILE_NAME


def load_sbs_credentials(default_user: str = "", default_pass: str = "") -> Tuple[str, str]:
    path = get_credentials_path()
    if not path.exists():
        return default_user, default_pass
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        user = (data.get("sbs_user") or "").strip() or default_user
        pwd = (data.get("sbs_pass") or "").strip() or default_pass
        return user, pwd
    except Exception:
        return default_user, default_pass


def save_sbs_credentials(user: str, pwd: str) -> None:
    path = get_credentials_path()
    payload = {"sbs_user": user.strip(), "sbs_pass": pwd.strip()}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
