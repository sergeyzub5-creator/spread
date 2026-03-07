from __future__ import annotations

import time
from decimal import Decimal, InvalidOperation
from typing import Any

from app.core.bybit.http_client import BybitApiError, BybitV5HttpClient
from app.core.execution.bybit_linear_adapter import BybitLinearExecutionAdapter
from app.core.logging.logger_factory import get_logger
from app.core.models.account import ClosePositionsResult, ExchangeAccountSnapshot, ExchangeCredentials
from app.core.models.execution import ExecutionOrderRequest
from app.core.models.instrument import InstrumentId, InstrumentKey, InstrumentRouting, InstrumentSpec


class BybitAccountConnector:
    def __init__(self, timeout_seconds: float = 10.0) -> None:
        self._client_timeout_seconds = float(timeout_seconds)
        self._logger = get_logger("accounts.bybit")

    def connect(self, credentials: ExchangeCredentials) -> ExchangeAccountSnapshot:
        client = BybitV5HttpClient(credentials, timeout_seconds=self._client_timeout_seconds)
        wallet_payload = client.get("/v5/account/wallet-balance", params={"accountType": "UNIFIED"}, auth=True)
        positions = self._load_linear_positions(client)
        self._logger.info("bybit unified account verified")
        return self._build_snapshot(wallet_payload, positions)

    def close_all_positions(self, credentials: ExchangeCredentials) -> ClosePositionsResult:
        client = BybitV5HttpClient(credentials, timeout_seconds=self._client_timeout_seconds)
        positions = self._load_linear_positions(client)
        close_requests = self._extract_close_requests(positions)
        if not close_requests:
            snapshot = self.connect(credentials)
            return ClosePositionsResult(exchange="bybit", closed_count=0, closed_symbols=tuple(), account_snapshot=snapshot)

        self._logger.info(
            "bybit close all positions started: count=%s symbols=%s",
            len(close_requests),
            ",".join(request.instrument_id.symbol for request in close_requests),
        )
        adapter = BybitLinearExecutionAdapter(credentials)
        adapter.connect()
        failures: list[str] = []
        closed_symbols: list[str] = []
        try:
            for request in close_requests:
                try:
                    result = adapter.place_order(request)
                    self._logger.info(
                        "bybit close order ack: symbol=%s order_id=%s status=%s",
                        result.symbol,
                        result.order_id,
                        result.status,
                    )
                    closed_symbols.append(request.instrument_id.symbol)
                except Exception as exc:
                    failures.append(f"{request.instrument_id.symbol}: {exc}")
                    self._logger.error("bybit close order failed: symbol=%s error=%s", request.instrument_id.symbol, exc)
        finally:
            adapter.close()

        if failures:
            raise BybitApiError("; ".join(failures))

        snapshot = self._refresh_snapshot_after_close(credentials)
        self._logger.info(
            "bybit close all positions completed: count=%s symbols=%s",
            len(closed_symbols),
            ",".join(closed_symbols),
        )
        return ClosePositionsResult(
            exchange="bybit",
            closed_count=len(closed_symbols),
            closed_symbols=tuple(closed_symbols),
            account_snapshot=snapshot,
        )

    def _refresh_snapshot_after_close(self, credentials: ExchangeCredentials) -> ExchangeAccountSnapshot:
        self._logger.info("bybit snapshot refresh after close started")
        client = BybitV5HttpClient(credentials, timeout_seconds=self._client_timeout_seconds)
        wallet_payload = client.get("/v5/account/wallet-balance", params={"accountType": "UNIFIED"}, auth=True)
        positions: list[dict[str, Any]] = []
        for _attempt in range(6):
            positions = self._load_linear_positions(client)
            if self._count_open_positions(positions) == 0:
                break
            time.sleep(0.5)
        self._logger.info("bybit snapshot refresh after close completed: open_positions=%s", self._count_open_positions(positions))
        return self._build_snapshot(wallet_payload, positions)

    def _build_snapshot(self, wallet_payload: dict[str, Any], positions: list[dict[str, Any]]) -> ExchangeAccountSnapshot:
        wallet_result = wallet_payload.get("result", {}) if isinstance(wallet_payload, dict) else {}
        wallet_list = wallet_result.get("list", []) if isinstance(wallet_result, dict) else []
        account_info = wallet_list[0] if isinstance(wallet_list, list) and wallet_list else {}
        total_wallet_balance = self._decimal_value(account_info.get("totalWalletBalance"))
        total_equity = self._decimal_value(account_info.get("totalEquity"))
        unrealized_pnl = sum((self._decimal_value(item.get("unrealisedPnl")) for item in positions), Decimal("0"))
        balance_value = total_wallet_balance if total_wallet_balance != Decimal("0") else total_equity
        return ExchangeAccountSnapshot(
            exchange="bybit",
            status_text="Подключено · Futures",
            balance_text=f"Баланс: {self._fmt_decimal(balance_value)} USD",
            positions_text=self._positions_text(positions),
            pnl_text=self._format_pnl_text(unrealized_pnl, "USD"),
            spot_enabled=False,
            futures_enabled=True,
            can_trade=True,
            account_profile={
                "account_type": "unified",
                "account_mode": "uta",
                "supports_spot": False,
                "supports_futures": True,
                "preferred_execution_route": "bybit_linear_trade_ws",
                "detected_via": ["v5_wallet_balance", "v5_position_list"],
            },
        )

    def _load_linear_positions(self, client: BybitV5HttpClient) -> list[dict[str, Any]]:
        positions: list[dict[str, Any]] = []
        for settle_coin in ("USDT", "USDC"):
            cursor: str | None = None
            while True:
                params: dict[str, Any] = {"category": "linear", "settleCoin": settle_coin, "limit": 200}
                if cursor:
                    params["cursor"] = cursor
                payload = client.get("/v5/position/list", params=params, auth=True)
                result = payload.get("result", {}) if isinstance(payload, dict) else {}
                items = result.get("list", []) if isinstance(result, dict) else []
                for item in items:
                    if isinstance(item, dict):
                        positions.append(item)
                cursor = str(result.get("nextPageCursor", "")).strip()
                if not cursor:
                    break
        return positions

    def _extract_close_requests(self, positions: list[dict[str, Any]]) -> list[ExecutionOrderRequest]:
        requests: list[ExecutionOrderRequest] = []
        for position in positions:
            symbol = str(position.get("symbol", "")).strip().upper()
            side = str(position.get("side", "")).strip().capitalize()
            size = self._decimal_value(position.get("size"))
            position_idx = self._int_or_zero(position.get("positionIdx"))
            if not symbol or side not in {"Buy", "Sell"} or size <= Decimal("0"):
                continue
            requests.append(
                ExecutionOrderRequest(
                    instrument_id=self._linear_instrument_stub(symbol, settle_asset=str(position.get("settleCoin", "USDT"))),
                    side="SELL" if side == "Buy" else "BUY",
                    order_type="MARKET",
                    quantity=size,
                    position_idx=position_idx,
                    reduce_only=True,
                    response_type="ACK",
                )
            )
        return requests

    @staticmethod
    def _linear_instrument_stub(symbol: str, *, settle_asset: str) -> InstrumentId:
        normalized_symbol = str(symbol or "").strip().upper()
        return InstrumentId(
            key=InstrumentKey(exchange="bybit", market_type="linear_perp", symbol=normalized_symbol),
            spec=InstrumentSpec(
                base_asset="",
                quote_asset="",
                contract_type="linear_perpetual",
                settle_asset=str(settle_asset or "USDT"),
                price_precision=Decimal("0"),
                qty_precision=Decimal("0"),
                min_qty=Decimal("0"),
                min_notional=Decimal("0"),
            ),
            routing=InstrumentRouting(
                ws_channel="orderbook.1",
                ws_symbol=normalized_symbol,
                order_route="bybit_linear_trade_ws",
            ),
        )

    @staticmethod
    def _count_open_positions(positions: list[dict[str, Any]]) -> int:
        count = 0
        for item in positions:
            if BybitAccountConnector._decimal_value(item.get("size")) > Decimal("0"):
                count += 1
        return count

    @staticmethod
    def _positions_text(positions: list[dict[str, Any]]) -> str:
        long_count = 0
        short_count = 0
        for item in positions:
            side = str(item.get("side", "")).strip().capitalize()
            size = BybitAccountConnector._decimal_value(item.get("size"))
            if size <= Decimal("0"):
                continue
            if side == "Buy":
                long_count += 1
            elif side == "Sell":
                short_count += 1
        if long_count <= 0 and short_count <= 0:
            return "Позиции: 0"
        parts: list[str] = []
        if long_count > 0:
            parts.append(f"<span style='color:#22c55e;'>{long_count} лонг</span>")
        if short_count > 0:
            parts.append(f"<span style='color:#ef4444;'>{short_count} шорт</span>")
        return "Позиции: " + "  ".join(parts)

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
    def _int_or_zero(value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _fmt_decimal(value: Decimal) -> str:
        return format(value.quantize(Decimal("0.01")), "f")
