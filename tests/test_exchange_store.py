from __future__ import annotations

import shutil
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from app.ui import exchange_store


class ExchangeStoreTests(unittest.TestCase):
    def test_save_exchange_cards_sanitizes_plaintext_and_resolves_secure_credentials(self) -> None:
        temp_dir = Path("tests") / "_tmp" / f"exchange-store-{uuid4().hex}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        exchanges_path = temp_dir / "exchanges.json"
        saved_credentials: dict[str, dict[str, str]] = {}
        card = {
            "exchange_code": "binance",
            "label": "Binance",
            "api_key": "test-api-key-1234",
            "api_secret": "secret-5678",
            "api_passphrase": "",
        }

        def fake_save_exchange_credentials(*, exchange_code: str, api_key: str, api_secret: str, api_passphrase: str = "", credential_ref: str | None = None) -> str:
            ref = credential_ref or "cred-binance"
            saved_credentials[ref] = {
                "exchange_code": exchange_code,
                "api_key": api_key,
                "api_secret": api_secret,
                "api_passphrase": api_passphrase,
            }
            return ref

        def fake_load_exchange_credentials(credential_ref: str | None) -> dict[str, str] | None:
            if credential_ref is None:
                return None
            return saved_credentials.get(credential_ref)

        try:
            with (
                patch.object(exchange_store, "_STORE_PATH", exchanges_path),
                patch.object(exchange_store, "save_exchange_credentials", side_effect=fake_save_exchange_credentials),
                patch.object(exchange_store, "load_exchange_credentials", side_effect=fake_load_exchange_credentials),
                patch.object(exchange_store, "has_exchange_credentials", side_effect=lambda ref: ref in saved_credentials),
                patch.object(exchange_store, "masked_api_key", side_effect=lambda ref: "test...1234" if ref in saved_credentials else ""),
            ):
                exchange_store.save_exchange_cards([card])
                cards = exchange_store.load_exchange_cards()
                self.assertEqual(len(cards), 1)
                stored_card = cards[0]
                self.assertNotIn("api_key", stored_card)
                self.assertNotIn("api_secret", stored_card)
                self.assertIn("credential_ref", stored_card)
                self.assertTrue(stored_card["credentials_stored"])
                resolved = exchange_store.resolve_exchange_card_credentials(stored_card)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

        self.assertIsNotNone(resolved)
        self.assertEqual(resolved["exchange_code"], "binance")
        self.assertEqual(resolved["api_key"], "test-api-key-1234")
        self.assertEqual(resolved["api_secret"], "secret-5678")


if __name__ == "__main__":
    unittest.main()
