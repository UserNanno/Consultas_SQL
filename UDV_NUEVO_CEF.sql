from __future__ import annotations
import json
import os
from pathlib import Path
from typing import Optional


def _app_dir() -> Path:
    base = Path(os.environ.get("LOCALAPPDATA", Path.home()))
    return base / "PrismaProject"


def _analyst_path() -> Path:
    d = _app_dir()
    d.mkdir(parents=True, exist_ok=True)
    return d / "analyst.json"


def load_matanalista(default: str = "") -> str:
    p = _analyst_path()
    if not p.exists():
        return default
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return (data.get("matanalista") or default).strip()
    except Exception:
        return default


def save_matanalista(matanalista: str) -> None:
    p = _analyst_path()
    payload = {"matanalista": (matanalista or "").strip()}
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
