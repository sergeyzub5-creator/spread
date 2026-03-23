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
    PAPI_BASE_URL = "https://papi.binance.com"
    SPOT_TIME_PATH = "/api/v3/time"
    FUTURES_TIME_PATH = "/fapi/v1/time"
    SPOT_ACCOUNT_PATH = "/api/v3/account"
    FUTURES_ACCOUNT_PATH = "/fapi/v3/account"
    PAPI_ACCOUNT_PATH = "/papi/v1/account"
    PAPI_UM_ACCOUNT_PATH = "/papi/v1/um/account"
    FUTURES_POSITION_MODE_PATH = "/fapi/v1/positionSide/dual"
    CLOSE_REFRESH_TIMEOUT_SECONDS = 20.0
    CLOSE_REFRESH_POLL_SECONDS = 0.35

    def __init__(self, timeout_seconds: float = 10.0) -> None:
        self.timeout_seconds = float(timeout_seconds)
        self._logger = get_logger("accounts.binance")

    def connect(self, credentials: ExchangeCredentials) -> ExchangeAccountSnapshot:
        spot_payload = None
        futures_payload = None
        portfolio_payload = None
        portfolio_um_payload = None
        spot_error: Exception | None = None
        derivatives_error: Exception | None = None

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
            derivatives_mode, derivatives_payloads = self._load_derivatives_account(credentials)
            futures_payload = derivatives_payloads.get("futures")
            portfolio_payload = derivatives_payloads.get("portfolio")
            portfolio_um_payload = derivatives_payloads.get("portfolio_um")
            if derivatives_mode == "portfolio_margin":
                self._logger.info("binance account type detected: portfolio margin")
            elif derivatives_mode == "usdm_futures":
                self._logger.info("binance account type detected: separate usdm futures")
        except Exception as exc:
            derivatives_error = exc
            self._logger.warning("binance derivatives account check failed: %s", exc)

        if spot_payload is None and futures_payload is None and portfolio_um_payload is None:
            raise BinanceApiError(self._format_connection_error(spot_error, derivatives_error))

        return self._build_snapshot(spot_payload, futures_payload, portfolio_payload, portfolio_um_payload)

    def _load_derivatives_account(self, credentials: ExchangeCredentials) -> tuple[str, dict[str, dict[str, Any]]]:
        profile = dict(getattr(credentials, "account_profile", {}) or {})
        hinted_type = str(profile.get("account_type") or profile.get("account_mode") or "").strip().lower()

        if hinted_type in {"portfolio_margin", "portfolio"}:
            portfolio_payload, portfolio_um_payload = self._load_portfolio_margin_account(credentials)
            return "portfolio_margin", {
                "portfolio": portfolio_payload,
                "portfolio_um": portfolio_um_payload,
            }

        if hinted_type in {"separate_spot_and_usdm", "classic", "usdm_futures"}:
            futures_payload = self._load_classic_futures_account(credentials)
            return "usdm_futures", {"futures": futures_payload}

        portfolio_probe_error: Exception | None = None
        try:
            portfolio_payload, portfolio_um_payload = self._load_portfolio_margin_account(credentials)
            return "portfolio_margin", {
                "portfolio": portfolio_payload,
                "portfolio_um": portfolio_um_payload,
            }
        except Exception as exc:
            portfolio_probe_error = exc

        try:
            futures_payload = self._load_classic_futures_account(credentials)
            return "usdm_futures", {"futures": futures_payload}
        except Exception as exc:
            raise BinanceApiError(self._format_connection_error(None, exc, portfolio_probe_error)) from exc

    def _load_classic_futures_account(self, credentials: ExchangeCredentials) -> dict[str, Any]:
        futures_payload = self._signed_get(
            base_url=self.FUTURES_BASE_URL,
            time_path=self.FUTURES_TIME_PATH,
            path=self.FUTURES_ACCOUNT_PATH,
            credentials=credentials,
        )
        self._logger.info("binance futures account verified")
        return futures_payload

    def _load_portfolio_margin_account(
        self,
        credentials: ExchangeCredentials,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        portfolio_payload = self._signed_get(
            base_url=self.PAPI_BASE_URL,
            time_base_url=self.SPOT_BASE_URL,
            time_path=self.SPOT_TIME_PATH,
            path=self.PAPI_ACCOUNT_PATH,
            credentials=credentials,
        )
        portfolio_um_payload = self._signed_get(
            base_url=self.PAPI_BASE_URL,
            time_base_url=self.SPOT_BASE_URL,
            time_path=self.SPOT_TIME_PATH,
            path=self.PAPI_UM_ACCOUNT_PATH,
            credentials=credentials,
        )
        self._logger.info("binance portfolio margin account verified")
        return portfolio_payload, portfolio_um_payload

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
                    if self._is_reduce_only_rejected(exc):
                        # Final tail often returns reduce-only reject when exchange already
                        # considers the position flat; do not fail the whole close flow.
                        self._logger.warning(
                            "binance close order reduce-only rejected treated as already flat: symbol=%s error=%s",
                            request.instrument_id.symbol,
                            exc,
                        )
                        closed_symbols.append(request.instrument_id.symbol)
                        continue
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
        portfolio_payload: dict[str, Any] | None = None,
        portfolio_um_payload: dict[str, Any] | None = None,
    ) -> ExchangeAccountSnapshot:
        spot_enabled = spot_payload is not None
        futures_enabled = futures_payload is not None

        if futures_payload is not None:
            wallet_balance = self._decimal_value(futures_payload.get("totalWalletBalance"))
            positions = futures_payload.get("positions", [])
            unrealized_pnl = self._open_positions_unrealized_pnl(positions)
            can_trade = bool(futures_payload.get("canTrade", True))
            return ExchangeAccountSnapshot(
                exchange="binance",
                status_text=self._status_text(spot_enabled, futures_enabled, can_trade),
                balance_text=f"Баланс: {self._fmt_decimal(wallet_balance)} USDT",
                positions_text=self._positions_text(positions),
                pnl_text=self._format_pnl_text(unrealized_pnl, "USDT"),
                spot_enabled=spot_enabled,
                futures_enabled=futures_enabled,
                can_trade=can_trade,
                account_profile={
                    "account_type": "separate_spot_and_usdm",
                    "account_mode": "classic",
                    "supports_spot": spot_enabled,
                    "supports_futures": futures_enabled,
                    "preferred_execution_route": "binance_usdm_trade_ws" if futures_enabled else None,
                    "detected_via": ["spot_account", "futures_account"],
                },
            )

        if portfolio_um_payload is not None:
            portfolio_positions = portfolio_um_payload.get("positions", [])
            wallet_balance = self._first_decimal(
                portfolio_um_payload,
                "totalWalletBalance",
                "totalMarginBalance",
                "totalCrossWalletBalance",
            )
            account_equity = self._first_decimal(
                portfolio_payload or {},
                "accountEquity",
                "totalWalletBalance",
                "totalMarginBalance",
            )
            balance_value = account_equity if account_equity != Decimal("0") else wallet_balance
            unrealized_pnl = self._open_positions_unrealized_pnl(portfolio_positions)
            can_trade = bool(portfolio_um_payload.get("canTrade", True))
            return ExchangeAccountSnapshot(
                exchange="binance",
                status_text=self._status_text(spot_enabled, True, can_trade, account_label="Portfolio Margin"),
                balance_text=f"Баланс: {self._fmt_decimal(balance_value)} USD",
                positions_text=self._positions_text(portfolio_positions),
                pnl_text=self._format_pnl_text(unrealized_pnl, "USDT"),
                spot_enabled=spot_enabled,
                futures_enabled=True,
                can_trade=can_trade,
                account_profile={
                    "account_type": "portfolio_margin",
                    "account_mode": "portfolio_margin",
                    "supports_spot": spot_enabled,
                    "supports_futures": True,
                    "preferred_execution_route": "binance_usdm_trade_ws",
                    "detected_via": ["spot_account", "papi_account", "papi_um_account"],
                },
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
            pnl_text=self._format_pnl_text(Decimal("0"), "USDT"),
            spot_enabled=spot_enabled,
            futures_enabled=futures_enabled,
            can_trade=can_trade,
            account_profile={
                "account_type": "spot_only",
                "account_mode": "classic",
                "supports_spot": spot_enabled,
                "supports_futures": futures_enabled,
                "preferred_execution_route": None,
                "detected_via": ["spot_account"],
            },
        )

    @staticmethod
    def _status_text(
        spot_enabled: bool,
        futures_enabled: bool,
        can_trade: bool,
        account_label: str | None = None,
    ) -> str:
        segments: list[str] = []
        if spot_enabled:
            segments.append("Spot")
        if futures_enabled:
            segments.append("Futures")
        suffix = " + ".join(segments) if segments else "API"
        if account_label:
            suffix = f"{suffix} · {account_label}"
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

    @classmethod
    def _first_decimal(cls, payload: dict[str, Any], *keys: str) -> Decimal:
        for key in keys:
            if key in payload:
                value = cls._decimal_value(payload.get(key))
                if value != Decimal("0"):
                    return value
        for key in keys:
            if key in payload:
                return cls._decimal_value(payload.get(key))
        return Decimal("0")

    def _signed_get(
        self,
        *,
        base_url: str,
        time_path: str,
        path: str,
        credentials: ExchangeCredentials,
        time_base_url: str | None = None,
    ) -> dict[str, Any]:
        return self._signed_request(
            method="GET",
            base_url=base_url,
            time_base_url=time_base_url,
            time_path=time_path,
            path=path,
            credentials=credentials,
        )

    def _signed_request(
        self,
        *,
        method: str,
        base_url: str,
        time_base_url: str | None,
        time_path: str,
        path: str,
        credentials: ExchangeCredentials,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        server_time_payload = self._public_get(base_url=time_base_url or base_url, path=time_path)
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
    def _format_connection_error(
        spot_error: Exception | None,
        futures_error: Exception | None,
        portfolio_error: Exception | None = None,
    ) -> str:
        parts: list[str] = []
        if spot_error:
            parts.append(f"Spot: {spot_error}")
        if futures_error:
            parts.append(f"Futures: {futures_error}")
        if portfolio_error:
            parts.append(f"Portfolio Margin: {portfolio_error}")
        if parts:
            return "; ".join(parts)
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
        deadline = time.time() + self.CLOSE_REFRESH_TIMEOUT_SECONDS
        while True:
            last_futures_payload = self._signed_get(
                base_url=self.FUTURES_BASE_URL,
                time_path=self.FUTURES_TIME_PATH,
                path=self.FUTURES_ACCOUNT_PATH,
                credentials=credentials,
            )
            if self._count_open_positions(last_futures_payload.get("positions", [])) == 0:
                break
            if time.time() >= deadline:
                break
            time.sleep(self.CLOSE_REFRESH_POLL_SECONDS)

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

    @staticmethod
    def _is_reduce_only_rejected(exc: Exception) -> bool:
        message = str(exc or "").lower()
        return "reduceonly" in message and "rejected" in message

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

    def _positions_text(self, positions: Any) -> str:
        if not isinstance(positions, list):
            return "Позиции: 0"
        long_count = 0
        short_count = 0
        for position in positions:
            if not isinstance(position, dict):
                continue
            position_amt = self._decimal_value(position.get("positionAmt"))
            if position_amt == Decimal("0"):
                continue
            position_side = str(position.get("positionSide", "")).strip().upper()
            if position_side == "LONG" or ((not position_side or position_side == "BOTH") and position_amt > 0):
                long_count += 1
            elif position_side == "SHORT" or ((not position_side or position_side == "BOTH") and position_amt < 0):
                short_count += 1
        return self._format_directional_positions_text(long_count, short_count)

    @staticmethod
    def _format_directional_positions_text(long_count: int, short_count: int) -> str:
        if long_count <= 0 and short_count <= 0:
            return "Позиции: 0"
        parts: list[str] = []
        if long_count > 0:
            parts.append(f"<span style='color:#22c55e;'>{long_count} лонг</span>")
        if short_count > 0:
            parts.append(f"<span style='color:#ef4444;'>{short_count} шорт</span>")
        return "Позиции: " + "  ".join(parts)

    def _open_positions_unrealized_pnl(self, positions: Any) -> Decimal:
        total = Decimal("0")
        if not isinstance(positions, list):
            return total
        for position in positions:
            if not isinstance(position, dict):
                continue
            if self._decimal_value(position.get("positionAmt")) == Decimal("0"):
                continue
            total += self._decimal_value(position.get("unrealizedProfit"))
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
