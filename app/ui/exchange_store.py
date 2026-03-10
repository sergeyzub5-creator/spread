from __future__ import annotations

import json
from pathlib import Path

from app.ui.secure_credential_store import has_exchange_credentials, load_exchange_credentials, masked_api_key, save_exchange_credentials


_STORE_PATH = Path(__file__).resolve().parents[1] / "data" / "exchanges.json"


def _sanitize_card(card: dict) -> dict:
    sanitized = dict(card)
    exchange_code = str(sanitized.get("exchange_code", "")).strip().lower()
    credential_ref = str(sanitized.get("credential_ref", "")).strip() or None
    api_key = str(sanitized.pop("api_key", "")).strip()
    api_secret = str(sanitized.pop("api_secret", "")).strip()
    api_passphrase = str(sanitized.pop("api_passphrase", "")).strip()
    if api_key and api_secret:
        credential_ref = save_exchange_credentials(
            exchange_code=exchange_code,
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
            credential_ref=credential_ref,
        )
    if credential_ref:
        sanitized["credential_ref"] = credential_ref
        sanitized["credentials_stored"] = has_exchange_credentials(credential_ref)
        sanitized["api_key_masked"] = masked_api_key(credential_ref)
    else:
        sanitized.pop("credential_ref", None)
        sanitized["credentials_stored"] = False
        sanitized["api_key_masked"] = ""
    return sanitized


def _write_payload(cards: list[dict]) -> None:
    _STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {"cards": cards}
    _STORE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_exchange_cards() -> list[dict]:
    if not _STORE_PATH.exists():
        return []
    try:
        payload = json.loads(_STORE_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return []
    raw_cards = payload.get("cards")
    if not isinstance(raw_cards, list):
        return []
    original_cards = [card for card in raw_cards if isinstance(card, dict)]
    sanitized_cards = [_sanitize_card(card) for card in original_cards]
    if sanitized_cards != original_cards:
        _write_payload(sanitized_cards)
    return sanitized_cards


def save_exchange_cards(cards: list[dict]) -> None:
    sanitized_cards = [_sanitize_card(card) for card in cards if isinstance(card, dict)]
    _write_payload(sanitized_cards)


def resolve_exchange_card_credentials(card: dict) -> dict | None:
    if not isinstance(card, dict):
        return None
    credential_ref = str(card.get("credential_ref", "")).strip()
    if credential_ref:
        credentials = load_exchange_credentials(credential_ref)
        if credentials is not None:
            return credentials
    api_key = str(card.get("api_key", "")).strip()
    api_secret = str(card.get("api_secret", "")).strip()
    if not api_key or not api_secret:
        return None
    return {
        "exchange_code": str(card.get("exchange_code", "")).strip().lower(),
        "api_key": api_key,
        "api_secret": api_secret,
        "api_passphrase": str(card.get("api_passphrase", "")).strip(),
    }
