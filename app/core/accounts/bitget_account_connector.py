from __future__ import annotations

import time
from decimal import Decimal, InvalidOperation
from typing import Any

from app.core.bitget.http_client import BitgetApiError, BitgetSignedHttpClient
from app.core.logging.logger_factory import get_logger
from app.core.models.account import ClosePositionsResult, ExchangeAccountSnapshot, ExchangeCredentials


class BitgetAccountConnector:
    SPOT_ASSETS_PATH = "/api/v2/spot/account/assets"
    FUTURES_ACCOUNTS_PATH = "/api/v2/mix/account/accounts"
    FUTURES_POSITIONS_PATH = "/api/v2/mix/position/all-position"
    FUTURES_CLOSE_POSITIONS_PATH = "/api/v2/mix/order/close-positions"
    SWITCH_STATUS_PATH = "/api/v3/account/switch-status"
    PRODUCT_TYPE = "USDT-FUTURES"
    MARGIN_COIN = "USDT"

    def __init__(self, timeout_seconds: float = 10.0) -> None:
        self._timeout_seconds = float(timeout_seconds)
        self._logger = get_logger("accounts.bitget")
        self._account_profile_cache: dict[str, dict[str, Any]] = {}

    def connect(self, credentials: ExchangeCredentials) -> ExchangeAccountSnapshot:
        client = BitgetSignedHttpClient(credentials, timeout_seconds=self._timeout_seconds)
        spot_assets: list[dict[str, Any]] | None = None
        futures_accounts: list[dict[str, Any]] | None = None
        positions: list[dict[str, Any]] | None = None
        spot_error: Exception | None = None
        futures_error: Exception | None = None

        try:
            spot_assets = client.get(self.SPOT_ASSETS_PATH, params={"assetType": "all"}).get("data", [])
            self._logger.info("bitget spot account verified")
        except Exception as exc:
            spot_error = exc
            self._logger.warning("bitget spot account check failed: %s", exc)

        try:
            futures_accounts = client.get(self.FUTURES_ACCOUNTS_PATH, params={"productType": self.PRODUCT_TYPE}).get("data", [])
            self._logger.info("bitget futures account verified")
            positions = client.get(
                self.FUTURES_POSITIONS_PATH,
                params={"productType": self.PRODUCT_TYPE, "marginCoin": self.MARGIN_COIN},
            ).get("data", [])
        except Exception as exc:
            futures_error = exc
            self._logger.warning("bitget futures account check failed: %s", exc)

        if spot_assets is None and futures_accounts is None:
            raise BitgetApiError(self._format_connection_error(spot_error, futures_error))

        account_profile = self._resolve_account_profile(credentials, client, spot_assets or [], futures_accounts or [])
        return self._build_snapshot(spot_assets or [], futures_accounts or [], positions or [], account_profile)

    def close_all_positions(self, credentials: ExchangeCredentials) -> ClosePositionsResult:
        client = BitgetSignedHttpClient(credentials, timeout_seconds=self._timeout_seconds)
        positions = client.get(
            self.FUTURES_POSITIONS_PATH,
            params={"productType": self.PRODUCT_TYPE, "marginCoin": self.MARGIN_COIN},
        ).get("data", [])
        close_targets = self._extract_close_targets(positions)
        if not close_targets:
            snapshot = self.connect(credentials)
            return ClosePositionsResult(exchange="bitget", closed_count=0, closed_symbols=tuple(), account_snapshot=snapshot)

        self._logger.info(
            "bitget close all positions started: count=%s symbols=%s",
            len(close_targets),
            ",".join(target["symbol"] for target in close_targets),
        )

        failures: list[str] = []
        closed_symbols: list[str] = []
        for target in close_targets:
            body = {
                "symbol": target["symbol"],
                "productType": self.PRODUCT_TYPE,
            }
            hold_side = str(target.get("holdSide", "")).strip().lower()
            if hold_side:
                body["holdSide"] = hold_side
            try:
                result = client.post(self.FUTURES_CLOSE_POSITIONS_PATH, body=body)
                self._logger.info("bitget close positions ack: symbol=%s response=%s", target["symbol"], result.get("msg"))
                closed_symbols.append(target["symbol"])
            except Exception as exc:
                failures.append(f"{target['symbol']}: {exc}")
                self._logger.error("bitget close positions failed: symbol=%s error=%s", target["symbol"], exc)

        if failures:
            raise BitgetApiError("; ".join(failures))

        snapshot = self._refresh_snapshot_after_close(credentials)
        self._logger.info(
            "bitget close all positions completed: count=%s symbols=%s",
            len(closed_symbols),
            ",".join(closed_symbols),
        )
        return ClosePositionsResult(
            exchange="bitget",
            closed_count=len(closed_symbols),
            closed_symbols=tuple(closed_symbols),
            account_snapshot=snapshot,
        )

    def _refresh_snapshot_after_close(self, credentials: ExchangeCredentials) -> ExchangeAccountSnapshot:
        self._logger.info("bitget snapshot refresh after close started")
        client = BitgetSignedHttpClient(credentials, timeout_seconds=self._timeout_seconds)
        spot_assets = client.get(self.SPOT_ASSETS_PATH, params={"assetType": "all"}).get("data", [])
        futures_accounts = client.get(self.FUTURES_ACCOUNTS_PATH, params={"productType": self.PRODUCT_TYPE}).get("data", [])
        positions: list[dict[str, Any]] = []
        for _attempt in range(6):
            positions = client.get(
                self.FUTURES_POSITIONS_PATH,
                params={"productType": self.PRODUCT_TYPE, "marginCoin": self.MARGIN_COIN},
            ).get("data", [])
            if self._count_open_positions(positions) == 0:
                break
            time.sleep(0.5)
        self._logger.info("bitget snapshot refresh after close completed: open_positions=%s", self._count_open_positions(positions))
        account_profile = self._resolve_account_profile(credentials, client, spot_assets, futures_accounts)
        return self._build_snapshot(spot_assets, futures_accounts, positions, account_profile)

    def _resolve_account_profile(
        self,
        credentials: ExchangeCredentials,
        client: BitgetSignedHttpClient,
        spot_assets: list[dict[str, Any]],
        futures_accounts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        api_key = str(credentials.api_key or "").strip()
        if credentials.account_profile:
            profile = dict(credentials.account_profile)
            if api_key:
                self._account_profile_cache[api_key] = dict(profile)
            return profile
        if api_key and api_key in self._account_profile_cache:
            return dict(self._account_profile_cache[api_key])
        profile = self._detect_account_profile(client, spot_assets, futures_accounts)
        if api_key:
            self._account_profile_cache[api_key] = dict(profile)
        return profile

    def _build_snapshot(
        self,
        spot_assets: list[dict[str, Any]],
        futures_accounts: list[dict[str, Any]],
        positions: list[dict[str, Any]],
        account_profile: dict[str, Any],
    ) -> ExchangeAccountSnapshot:
        spot_usdt = Decimal("0")
        funded_assets = 0
        for asset in spot_assets if isinstance(spot_assets, list) else []:
            if not isinstance(asset, dict):
                continue
            available = self._decimal_value(asset.get("available"))
            frozen = self._decimal_value(asset.get("frozen"))
            locked = self._decimal_value(asset.get("locked"))
            total = available + frozen + locked
            if total > Decimal("0"):
                funded_assets += 1
            if str(asset.get("coin", "")).strip().upper() == "USDT":
                spot_usdt = total

        futures_account = None
        for item in futures_accounts if isinstance(futures_accounts, list) else []:
            if not isinstance(item, dict):
                continue
            if str(item.get("marginCoin", "")).strip().upper() == self.MARGIN_COIN:
                futures_account = item
                break
        if futures_account is None and isinstance(futures_accounts, list) and futures_accounts:
            first = futures_accounts[0]
            if isinstance(first, dict):
                futures_account = first

        balance_equity = self._decimal_value((futures_account or {}).get("usdtEquity"))
        unrealized_pnl = self._open_positions_unrealized_pnl(positions)
        if balance_equity <= Decimal("0"):
            balance_text = (
                f"Баланс: {self._fmt_decimal(spot_usdt)} USDT"
                if spot_usdt > Decimal("0")
                else f"Баланс: активов {funded_assets}"
            )
        else:
            balance_text = f"Баланс: {self._fmt_decimal(balance_equity)} USDT"

        spot_enabled = bool(spot_assets)
        futures_enabled = bool(futures_accounts)

        return ExchangeAccountSnapshot(
            exchange="bitget",
            status_text=self._status_text(spot_enabled, futures_enabled),
            balance_text=balance_text,
            positions_text=self._positions_text(positions),
            pnl_text=self._format_pnl_text(unrealized_pnl, "USDT"),
            spot_enabled=spot_enabled,
            futures_enabled=futures_enabled,
            can_trade=True,
            account_profile=account_profile,
        )

    def _detect_account_profile(
        self,
        client: BitgetSignedHttpClient,
        spot_assets: list[dict[str, Any]],
        futures_accounts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        switch_status_payload: dict[str, Any] | None = None
        switch_status_error: Exception | None = None
        try:
            switch_status_payload = client.get(self.SWITCH_STATUS_PATH)
        except Exception as exc:
            switch_status_error = exc
            error_text = str(exc).strip().lower()
            if "classic account mode" in error_text:
                self._logger.info("bitget switch-status indicates classic account mode")
            else:
                self._logger.warning("bitget switch-status detection failed: %s", exc)

        switch_data = switch_status_payload.get("data") if isinstance(switch_status_payload, dict) else None
        flattened = self._flatten_switch_data(switch_data)
        account_type = self._pick_first_non_empty(
            flattened,
            "accountType",
            "toAccountType",
            "targetAccountType",
            "type",
        ) or "unknown"
        account_mode = self._pick_first_non_empty(
            flattened,
            "assetMode",
            "marginMode",
            "mode",
            "tradeMode",
        ) or "unknown"
        switch_status = self._pick_first_non_empty(
            flattened,
            "switchStatus",
            "status",
            "switchState",
        ) or "unknown"

        if switch_status_error is not None:
            error_text = str(switch_status_error).strip().lower()
            if "classic account mode" in error_text:
                account_type = "classic"
                account_mode = "classic"
                switch_status = "classic_only"

        account_type_normalized = account_type.strip().lower()
        account_mode_normalized = account_mode.strip().lower()
        switch_status_normalized = switch_status.strip().lower()
        is_uta = any(token in account_type_normalized for token in ("uta", "unified")) or any(
            token in account_mode_normalized for token in ("multi", "unified", "portfolio")
        ) or switch_status_normalized in {"success", "switched", "complete"}

        preferred_execution_route = "bitget_linear_trade_ws"
        execution_stack = "uta_v3_private_ws" if is_uta else "classic_v2_private_ws"
        return {
            "account_type": "uta" if is_uta else account_type_normalized,
            "account_mode": account_mode_normalized,
            "switch_status": switch_status_normalized,
            "supports_spot": bool(spot_assets),
            "supports_futures": bool(futures_accounts),
            "preferred_execution_route": preferred_execution_route if futures_accounts else None,
            "execution_stack": execution_stack if futures_accounts else None,
            "detected_via": [
                "spot_assets_v2" if spot_assets else None,
                "futures_accounts_v2" if futures_accounts else None,
                "switch_status_v3" if switch_status_payload is not None else None,
            ],
            "switch_status_error": str(switch_status_error) if switch_status_error is not None else None,
            "raw_switch_status": switch_data if isinstance(switch_data, dict) else {},
        }

    def _extract_close_targets(self, positions: list[dict[str, Any]]) -> list[dict[str, str]]:
        targets: list[dict[str, str]] = []
        for item in positions if isinstance(positions, list) else []:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol", "")).strip().upper()
            total = self._decimal_value(item.get("total"))
            if not symbol or total <= Decimal("0"):
                continue
            pos_mode = str(item.get("posMode", "")).strip().lower()
            hold_side = str(item.get("holdSide", "")).strip().lower()
            target = {"symbol": symbol}
            if pos_mode == "hedge_mode" and hold_side in {"long", "short"}:
                target["holdSide"] = hold_side
            targets.append(target)
        return targets

    @staticmethod
    def _count_open_positions(positions: list[dict[str, Any]]) -> int:
        count = 0
        for item in positions if isinstance(positions, list) else []:
            if not isinstance(item, dict):
                continue
            if BitgetAccountConnector._decimal_value(item.get("total")) > Decimal("0"):
                count += 1
        return count

    @staticmethod
    def _positions_text(positions: list[dict[str, Any]]) -> str:
        long_count = 0
        short_count = 0
        for item in positions if isinstance(positions, list) else []:
            if not isinstance(item, dict):
                continue
            total = BitgetAccountConnector._decimal_value(item.get("total"))
            if total <= Decimal("0"):
                continue
            hold_side = str(item.get("holdSide", "")).strip().lower()
            if hold_side == "long":
                long_count += 1
            elif hold_side == "short":
                short_count += 1
            else:
                long_count += 1
        if long_count <= 0 and short_count <= 0:
            return "Позиции: 0"
        parts: list[str] = []
        if long_count > 0:
            parts.append(f"<span style='color:#22c55e;'>{long_count} лонг</span>")
        if short_count > 0:
            parts.append(f"<span style='color:#ef4444;'>{short_count} шорт</span>")
        return "Позиции: " + "  ".join(parts)

    @staticmethod
    def _open_positions_unrealized_pnl(positions: list[dict[str, Any]]) -> Decimal:
        total = Decimal("0")
        for item in positions if isinstance(positions, list) else []:
            if not isinstance(item, dict):
                continue
            if BitgetAccountConnector._decimal_value(item.get("total")) <= Decimal("0"):
                continue
            total += BitgetAccountConnector._decimal_value(item.get("unrealizedPL"))
        return total

    @classmethod
    def _format_pnl_text(cls, value: Decimal, unit: str) -> str:
        formatted = cls._fmt_decimal(value)
        if value > Decimal("0"):
            return f"PnL: <span style='color:#22c55e;'>{formatted} {unit}</span>"
        if value < Decimal("0"):
            return f"PnL: <span style='color:#ef4444;'>{formatted} {unit}</span>"
        return f"PnL: {formatted} {unit}"

    @staticmethod
    def _decimal_value(value: Any) -> Decimal:
        try:
            return Decimal(str(value or "0"))
        except (InvalidOperation, ValueError):
            return Decimal("0")

    @staticmethod
    def _fmt_decimal(value: Decimal) -> str:
        return format(value.quantize(Decimal("0.01")), "f")

    @staticmethod
    def _flatten_switch_data(data: Any) -> dict[str, Any]:
        if isinstance(data, dict):
            return data
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    return item
        return {}

    @staticmethod
    def _pick_first_non_empty(payload: dict[str, Any], *keys: str) -> str | None:
        for key in keys:
            value = payload.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return None

    @staticmethod
    def _status_text(spot_enabled: bool, futures_enabled: bool) -> str:
        segments: list[str] = []
        if spot_enabled:
            segments.append("Spot")
        if futures_enabled:
            segments.append("Futures")
        suffix = " + ".join(segments) if segments else "API"
        return f"Подключено · {suffix}"

    @staticmethod
    def _format_connection_error(spot_error: Exception | None, futures_error: Exception | None) -> str:
        if spot_error and futures_error:
            return f"Spot: {spot_error}; Futures: {futures_error}"
        if futures_error:
            return str(futures_error)
        if spot_error:
            return str(spot_error)
        return "Unknown Bitget connection error"
