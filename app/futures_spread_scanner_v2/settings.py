from __future__ import annotations

import json
import os
from pathlib import Path

_STATE_PATH = Path("app/futures_spread_scanner_v2/data/futures_spread_scanner_v2_settings.json")


def load_v2_settings() -> dict:
    if not _STATE_PATH.exists():
        return {}
    try:
        with _STATE_PATH.open("r", encoding="utf-8") as file:
            payload = json.load(file)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def save_v2_settings(**changes) -> dict:
    data = load_v2_settings()
    for key, value in changes.items():
        if value is None:
            data.pop(key, None)
        else:
            data[key] = value
    _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _STATE_PATH.with_suffix(_STATE_PATH.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
    os.replace(tmp_path, _STATE_PATH)
    return data


__all__ = ["load_v2_settings", "save_v2_settings"]
