from __future__ import annotations

import json
from pathlib import Path


_STORE_PATH = Path(__file__).resolve().parents[1] / "data" / "exchanges.json"


def load_exchange_cards() -> list[dict]:
    if not _STORE_PATH.exists():
        return []
    try:
        payload = json.loads(_STORE_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return []
    cards = payload.get("cards")
    if not isinstance(cards, list):
        return []
    return [card for card in cards if isinstance(card, dict)]


def save_exchange_cards(cards: list[dict]) -> None:
    _STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {"cards": cards}
    _STORE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

