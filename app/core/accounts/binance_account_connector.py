from __future__ import annotations

import hashlib
import hmac
import json
import time
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.core.execution.binance_usdm_adapter import BinanceUsdmExecutionAdapter
from app.core.logging.logger_factory import get_logger
from app.core.models.account import ClosePositionsResult, ExchangeAccountSnapshot, ExchangeCredentials
from app.core.models.execution import ExecutionOrderRequest
from app.core.models.instrument import InstrumentId, InstrumentKey, InstrumentRouting, InstrumentSpec


class BinanceApiError(RuntimeError):
    def __init__(self, message: str, code: int | None = None) -> None:
        super().__init__(message)
        self.code = code


class BinanceAccountConnector:
    SPOT_BASE_URL = "https://api.binance.com"
    FUTURES_BASE_URL = "https://fapi.binance.com"
    SPOT_TIME_PATH = "/api/v3/time"
    FUTURES_TIME_PATH = "/fapi/v1/time"
    SPOT_ACCOUNT_PATH = "/api/v3/account"
    FUTURES_ACCOUNT_PATH = "/fapi/v3/account"
    FUTURES_POSITION_MODE_PATH = "/fapi/v1/positionSide/dual"

    def __init__(self, timeout_seconds: float = 10.0) -> None:
        self.timeout_seconds = float(timeout_seconds)
        self._logger = get_logger("accounts.binance")

    def connect(self, credentials: ExchangeCredentials) -> ExchangeAccountSnapshot:
        spot_payload = None
        futures_payload = None
        spot_error: Exception | None = None
        futures_error: Exception | None = None

        try:
            spot_payload = self._signed_get(
                base_url=self.SPOT_BASE_URL,
                time_path=self.SPOT_TIME_PATH,
                path=self.SPOT_ACCOUNT_PATH,
                credentials=credentials,
            )
            self._logger.info("binance spot account verified")
        except Exception as exc:
            spot_error = exc
            self._logger.warning("binance spot account check failed: %s", exc)

        try:
            futures_payload = self._signed_get(
                base_url=self.FUTURES_BASE_URL,
                time_path=self.FUTURES_TIME_PATH,
                path=self.FUTURES_ACCOUNT_PATH,
                credentials=credentials,
            )
            self._logger.info("binance futures account verified")
        except Exception as exc:
            futures_error = exc
            self._logger.warning("binance futures account check failed: %s", exc)

        if spot_payload is None and futures_payload is None:
            raise BinanceApiError(self._format_connection_error(spot_error, futures_error))

        return self._build_snapshot(spot_payload, futures_payload)

    def close_all_positions(self, credentials: ExchangeCredentials) -> ClosePositionsResult:
        futures_payload = self._signed_get(
            base_url=self.FUTURES_BASE_URL,
            time_path=self.FUTURES_TIME_PATH,
            path=self.FUTURES_ACCOUNT_PATH,
            credentials=credentials,
        )
        if not bool(futures_payload.get("canTrade", True)):
            raise BinanceApiError("Futures trading is disabled for this API key")

        position_mode_payload = self._signed_get(
            base_url=self.FUTURES_BASE_URL,
            time_path=self.FUTURES_TIME_PATH,
            path=self.FUTURES_POSITION_MODE_PATH,
            credentials=credentials,
        )
        hedge_mode = bool(position_mode_payload.get("dualSidePosition", False))

        close_requests = self._extract_close_requests(futures_payload.get("positions", []), hedge_mode=hedge_mode)
        if not close_requests:
            snapshot = self.connect(credentials)
            return ClosePositionsResult(
                exchange="binance",
                closed_count=0,
                closed_symbols=tuple(),
                account_snapshot=snapshot,
            )

        self._logger.info(
            "binance close all positions started: hedge_mode=%s count=%s symbols=%s",
            hedge_mode,
            len(close_requests),
            ",".join(request.instrument_id.symbol for request in close_requests),
        )

        adapter = BinanceUsdmExecutionAdapter(credentials)
        adapter.connect()
        failures: list[str] = []
        closed_symbols: list[str] = []
        try:
            for request in close_requests:
                try:
                    result = adapter.place_order(request)
                    self._logger.info(
                        "binance close order ack: symbol=%s order_id=%s status=%s executed_qty=%s",
                        result.symbol,
                        result.order_id,
                        result.status,
                        result.executed_qty,
                    )
                    closed_symbols.append(request.instrument_id.symbol)
                except Exception as exc:
                    self._logger.error("binance close order failed: symbol=%s error=%s", request.instrument_id.symbol, exc)
                    failures.append(f"{request.instrument_id.symbol}: {exc}")
        finally:
            adapter.close()

        if failures:
            raise BinanceApiError("; ".join(failures))

        snapshot = self._refresh_snapshot_after_close(credentials)
        self._logger.info(
            "binance close all positions completed: count=%s symbols=%s",
            len(closed_symbols),
            ",".join(closed_symbols),
        )
        return ClosePositionsResult(
            exchange="binance",
            closed_count=len(closed_symbols),
            closed_symbols=tuple(closed_symbols),
            account_snapshot=snapshot,
        )

    def _build_snapshot(
        self,
        spot_payload: dict[str, Any] | None,
        futures_payload: dict[str, Any] | None,
    ) -> ExchangeAccountSnapshot:
        spot_enabled = spot_payload is not None
        futures_enabled = futures_payload is not None

        if futures_payload is not None:
            wallet_balance = self._decimal_value(futures_payload.get("totalWalletBalance"))
            unrealized_pnl = self._decimal_value(futures_payload.get("totalUnrealizedProfit"))
            open_positions = self._count_open_positions(futures_payload.get("positions", []))
            can_trade = bool(futures_payload.get("canTrade", True))
            return ExchangeAccountSnapshot(
                exchange="binance",
                status_text=self._status_text(spot_enabled, futures_enabled, can_trade),
                balance_text=f"Баланс: {self._fmt_decimal(wallet_balance)} USDT",
                positions_text=f"Позиции: {open_positions}",
                pnl_text=f"PnL: {self._fmt_decimal(unrealized_pnl)} USDT",
                spot_enabled=spot_enabled,
                futures_enabled=futures_enabled,
                can_trade=can_trade,
            )

        spot_balances = spot_payload.get("balances", []) if isinstance(spot_payload, dict) else []
        funded_assets = 0
        usdt_total = Decimal("0")
        if isinstance(spot_balances, list):
            for balance in spot_balances:
                if not isinstance(balance, dict):
                    continue
                free = self._decimal_value(balance.get("free"))
                locked = self._decimal_value(balance.get("locked"))
                total = free + locked
                if total != Decimal("0"):
                    funded_assets += 1
                if str(balance.get("asset", "")).upper() == "USDT":
                    usdt_total = total

        can_trade = bool(spot_payload.get("canTrade", True)) if isinstance(spot_payload, dict) else True
        balance_text = (
            f"Баланс: {self._fmt_decimal(usdt_total)} USDT"
            if usdt_total != Decimal("0")
            else f"Баланс: активов {funded_assets}"
        )
        return ExchangeAccountSnapshot(
            exchange="binance",
            status_text=self._status_text(spot_enabled, futures_enabled, can_trade),
            balance_text=balance_text,
            positions_text="Позиции: 0",
            pnl_text="PnL: 0.00 USDT",
            spot_enabled=spot_enabled,
            futures_enabled=futures_enabled,
            can_trade=can_trade,
        )

    @staticmethod
    def _status_text(spot_enabled: bool, futures_enabled: bool, can_trade: bool) -> str:
        segments: list[str] = []
        if spot_enabled:
            segments.append("Spot")
        if futures_enabled:
            segments.append("Futures")
        suffix = " + ".join(segments) if segments else "API"
        if not can_trade:
            return f"Подключено · {suffix} · read-only"
        return f"Подключено · {suffix}"

    @staticmethod
    def _fmt_decimal(value: Decimal) -> str:
        return format(value.quantize(Decimal("0.01")), "f")

    @staticmethod
    def _decimal_value(value: Any) -> Decimal:
        try:
            return Decimal(str(value or "0"))
        except (InvalidOperation, ValueError):
            return Decimal("0")

    def _signed_get(
        self,
        *,
        base_url: str,
        time_path: str,
        path: str,
        credentials: ExchangeCredentials,
    ) -> dict[str, Any]:
        return self._signed_request(
            method="GET",
            base_url=base_url,
            time_path=time_path,
            path=path,
            credentials=credentials,
        )

    def _signed_request(
        self,
        *,
        method: str,
        base_url: str,
        time_path: str,
        path: str,
        credentials: ExchangeCredentials,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        server_time_payload = self._public_get(base_url=base_url, path=time_path)
        timestamp = int(server_time_payload.get("serverTime", 0))
        signed_params = {key: value for key, value in (params or {}).items() if value is not None}
        signed_params["recvWindow"] = "5000"
        signed_params["timestamp"] = str(timestamp)
        query = urlencode(signed_params)
        signature = hmac.new(
            credentials.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        signed_query = f"{query}&signature={signature}"
        headers = {
            "User-Agent": "spread-sniper-ui-shell/1.0",
            "X-MBX-APIKEY": credentials.api_key,
        }
        request = Request(f"{base_url}{path}?{signed_query}", headers=headers, method=method)
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            payload = self._decode_error_payload(exc)
            raise BinanceApiError(self._error_message(payload, exc), code=payload.get("code")) from exc
        except URLError as exc:
            raise BinanceApiError(f"network error: {exc.reason}") from exc

    def _public_get(self, *, base_url: str, path: str) -> dict[str, Any]:
        request = Request(
            f"{base_url}{path}",
            headers={"User-Agent": "spread-sniper-ui-shell/1.0"},
            method="GET",
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            payload = self._decode_error_payload(exc)
            raise BinanceApiError(self._error_message(payload, exc), code=payload.get("code")) from exc
        except URLError as exc:
            raise BinanceApiError(f"network error: {exc.reason}") from exc

    @staticmethod
    def _decode_error_payload(error: HTTPError) -> dict[str, Any]:
        try:
            payload = json.loads(error.read().decode("utf-8"))
            if isinstance(payload, dict):
                return payload
        except Exception:
            pass
        return {}

    @staticmethod
    def _error_message(payload: dict[str, Any], error: HTTPError) -> str:
        code = payload.get("code")
        msg = payload.get("msg")
        if code is not None and msg:
            return f"[{code}] {msg}"
        return f"http {error.code}"

    @staticmethod
    def _format_connection_error(spot_error: Exception | None, futures_error: Exception | None) -> str:
        if spot_error and futures_error:
            return f"Spot: {spot_error}; Futures: {futures_error}"
        if futures_error:
            return str(futures_error)
        if spot_error:
            return str(spot_error)
        return "Unknown Binance connection error"

    def _refresh_snapshot_after_close(self, credentials: ExchangeCredentials) -> ExchangeAccountSnapshot:
        self._logger.info("binance snapshot refresh after close started")
        spot_payload = None
        try:
            spot_payload = self._signed_get(
                base_url=self.SPOT_BASE_URL,
                time_path=self.SPOT_TIME_PATH,
                path=self.SPOT_ACCOUNT_PATH,
                credentials=credentials,
            )
        except Exception as exc:
            self._logger.warning("binance spot account refresh after close failed: %s", exc)

        last_futures_payload: dict[str, Any] | None = None
        for _attempt in range(6):
            last_futures_payload = self._signed_get(
                base_url=self.FUTURES_BASE_URL,
                time_path=self.FUTURES_TIME_PATH,
                path=self.FUTURES_ACCOUNT_PATH,
                credentials=credentials,
            )
            if self._count_open_positions(last_futures_payload.get("positions", [])) == 0:
                break
            time.sleep(0.5)

        self._logger.info(
            "binance snapshot refresh after close completed: open_positions=%s",
            self._count_open_positions(last_futures_payload.get("positions", []) if isinstance(last_futures_payload, dict) else []),
        )
        return self._build_snapshot(spot_payload, last_futures_payload)

    def _extract_close_requests(self, positions: Any, *, hedge_mode: bool) -> list[ExecutionOrderRequest]:
        requests: list[ExecutionOrderRequest] = []
        if not isinstance(positions, list):
            return requests

        for position in positions:
            if not isinstance(position, dict):
                continue
            position_amt = self._decimal_value(position.get("positionAmt"))
            if position_amt == Decimal("0"):
                continue

            symbol = str(position.get("symbol", "")).strip().upper()
            if not symbol:
                continue

            position_side = None
            reduce_only: bool | None = None
            if hedge_mode:
                current_side = str(position.get("positionSide", "")).strip().upper()
                position_side = current_side if current_side in {"LONG", "SHORT"} else ("LONG" if position_amt > 0 else "SHORT")
            else:
                reduce_only = True

            requests.append(
                ExecutionOrderRequest(
                    instrument_id=self._futures_instrument_stub(symbol),
                    side="SELL" if position_amt > 0 else "BUY",
                    order_type="MARKET",
                    quantity=abs(position_amt),
                    position_side=position_side,
                    reduce_only=reduce_only,
                    response_type="RESULT",
                )
            )

        return requests

    def _count_open_positions(self, positions: Any) -> int:
        count = 0
        if not isinstance(positions, list):
            return count
        for position in positions:
            if not isinstance(position, dict):
                continue
            if self._decimal_value(position.get("positionAmt")) != Decimal("0"):
                count += 1
        return count

    @staticmethod
    def _futures_instrument_stub(symbol: str) -> InstrumentId:
        normalized_symbol = str(symbol or "").strip().upper()
        return InstrumentId(
            key=InstrumentKey(
                exchange="binance",
                market_type="linear_perp",
                symbol=normalized_symbol,
            ),
            spec=InstrumentSpec(
                base_asset="",
                quote_asset="",
                contract_type="perpetual",
                settle_asset="USDT",
                price_precision=Decimal("0"),
                qty_precision=Decimal("0"),
                min_qty=Decimal("0"),
                min_notional=Decimal("0"),
            ),
            routing=InstrumentRouting(
                ws_channel="bookTicker",
                ws_symbol=normalized_symbol.lower(),
                order_route="binance_usdm_trade_ws",
            ),
        )
