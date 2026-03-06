from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import Any

from app.core.bybit.http_client import BybitV5HttpClient
from app.core.execution.adapter import ExecutionAdapter
from app.core.execution.bybit_private_stream import BybitPrivateExecutionStream
from app.core.execution.bybit_trade_ws import BybitLinearTradeWebSocketTransport
from app.core.logging.logger_factory import get_logger
from app.core.models.account import ExchangeCredentials
from app.core.models.execution import ExecutionOrderRequest, ExecutionOrderResult, ExecutionStreamEvent


class BybitLinearExecutionAdapter(ExecutionAdapter):
    ROUTE_NAME = "bybit_linear_trade_ws"

    def __init__(self, credentials: ExchangeCredentials) -> None:
        self._logger = get_logger("execution.bybit_linear")
        self._transport = BybitLinearTradeWebSocketTransport(credentials)
        self._private_stream = BybitPrivateExecutionStream(credentials)
        self._client = BybitV5HttpClient(credentials)

    def route_name(self) -> str:
        return self.ROUTE_NAME

    def connect(self) -> None:
        self._transport.connect()
        self._logger.info("bybit execution adapter connected | route=%s", self.ROUTE_NAME)

    def on_execution_event(self, callback: Callable[[ExecutionStreamEvent], None]) -> None:
        self._private_stream.on_execution_event(callback)
        self._private_stream.connect()
        self._logger.info("bybit execution adapter private stream attached")

    def place_order(
        self,
        request: ExecutionOrderRequest,
        on_request_sent: Callable[[dict[str, Any]], None] | None = None,
    ) -> ExecutionOrderResult:
        self._assert_route(request.instrument_id.routing.order_route)
        payload = self._build_place_order_payload(request)
        response = self._transport.request("order.create", payload, on_request_sent=on_request_sent)
        result = self._normalize_result(response, request=request)
        self._logger.info(
            "bybit order.create ack | symbol=%s | order_id=%s | status=%s",
            result.symbol,
            result.order_id,
            result.status,
        )
        return result

    def cancel_order(
        self,
        *,
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> ExecutionOrderResult:
        payload: dict[str, Any] = {"category": "linear", "symbol": str(symbol).upper()}
        if order_id is not None:
            payload["orderId"] = str(order_id)
        if client_order_id:
            payload["orderLinkId"] = client_order_id
        response = self._transport.request("order.cancel", payload)
        return self._normalize_result(response, request=None)

    def query_order(
        self,
        *,
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> ExecutionOrderResult:
        params: dict[str, Any] = {"category": "linear", "symbol": str(symbol).upper()}
        if order_id is not None:
            params["orderId"] = str(order_id)
        if client_order_id:
            params["orderLinkId"] = client_order_id
        response = self._client.get("/v5/order/realtime", params=params, auth=True)
        result_list = response.get("result", {}).get("list", [])
        item = result_list[0] if isinstance(result_list, list) and result_list else {}
        wrapped = {"data": item, "retCode": response.get("retCode", 0), "retMsg": response.get("retMsg", "OK")}
        return self._normalize_result(wrapped, request=None)

    def close(self) -> None:
        self._transport.close()
        self._private_stream.close()
        self._logger.info("bybit execution adapter closed")

    def _build_place_order_payload(self, request: ExecutionOrderRequest) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "category": "linear",
            "symbol": request.instrument_id.symbol,
            "side": "Buy" if str(request.side).upper() == "BUY" else "Sell",
            "orderType": "Market" if str(request.order_type).upper() == "MARKET" else str(request.order_type).title(),
        }
        if request.quantity is not None:
            payload["qty"] = self._normalize_decimal(request.quantity)
        if request.price is not None:
            payload["price"] = self._normalize_decimal(request.price)
        if request.time_in_force:
            payload["timeInForce"] = request.time_in_force
        if request.position_idx is not None:
            payload["positionIdx"] = int(request.position_idx)
        if request.reduce_only is not None:
            payload["reduceOnly"] = bool(request.reduce_only)
        if request.new_client_order_id:
            payload["orderLinkId"] = request.new_client_order_id
        return payload

    def _normalize_result(self, response: dict[str, Any], request: ExecutionOrderRequest | None) -> ExecutionOrderResult:
        data = response.get("data", {}) if isinstance(response.get("data"), dict) else {}
        return ExecutionOrderResult(
            exchange="bybit",
            route=self.ROUTE_NAME,
            request_id=str(response.get("reqId") or response.get("id") or ""),
            symbol=str(data.get("symbol") or (request.instrument_id.symbol if request is not None else "")),
            order_id=self._str_or_none(data.get("orderId")),
            client_order_id=self._str_or_none(data.get("orderLinkId")),
            status=self._str_or_none(data.get("orderStatus")) or "ACCEPTED",
            side=self._str_or_none(data.get("side")) or ("Buy" if request and request.side.upper() == "BUY" else "Sell"),
            order_type=self._str_or_none(data.get("orderType")) or (request.order_type if request is not None else ""),
            position_side=self._str_or_none(data.get("positionIdx")),
            price=self._str_or_none(data.get("price")),
            original_qty=self._str_or_none(data.get("qty")),
            executed_qty=self._str_or_none(data.get("cumExecQty")),
            avg_price=self._str_or_none(data.get("avgPrice")),
            update_time=self._int_or_none(data.get("updatedTime")),
            raw=response,
        )

    @staticmethod
    def _normalize_decimal(value: Decimal) -> str:
        text = format(value, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text or "0"

    @staticmethod
    def _str_or_none(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value)
        return text if text else None

    @staticmethod
    def _int_or_none(value: Any) -> int | None:
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _assert_route(self, route_name: str) -> None:
        if route_name != self.ROUTE_NAME:
            raise ValueError(f"Unsupported execution route: {route_name}")
