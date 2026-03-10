from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import Any

from app.core.execution.adapter import ExecutionAdapter
from app.core.execution.binance_usdm_trade_ws import BinanceUsdmTradeWebSocketTransport
from app.core.execution.binance_usdm_user_data_stream import BinanceUsdmUserDataStream
from app.core.logging.logger_factory import get_logger
from app.core.models.account import ExchangeCredentials
from app.core.models.execution import ExecutionOrderRequest, ExecutionOrderResult, ExecutionStreamEvent


class BinanceUsdmExecutionAdapter(ExecutionAdapter):
    ROUTE_NAME = "binance_usdm_trade_ws"

    def __init__(self, credentials: ExchangeCredentials) -> None:
        self._logger = get_logger("execution.binance_usdm")
        self._transport = BinanceUsdmTradeWebSocketTransport(credentials)
        self._user_data_stream = BinanceUsdmUserDataStream(credentials)

    def route_name(self) -> str:
        return self.ROUTE_NAME

    def connect(self) -> None:
        self._transport.connect()
        self._logger.info("binance execution adapter connected | route=%s", self.ROUTE_NAME)

    def on_execution_event(self, callback: Callable[[ExecutionStreamEvent], None]) -> None:
        self._user_data_stream.on_execution_event(callback)
        self._user_data_stream.connect()
        self._logger.info("binance execution adapter user stream attached")

    def place_order(
        self,
        request: ExecutionOrderRequest,
        on_request_sent: Callable[[dict[str, Any]], None] | None = None,
    ) -> ExecutionOrderResult:
        self._assert_route(request.instrument_id.routing.order_route)
        params = self._build_place_order_params(request)
        response = self._transport.request("order.place", params, on_request_sent=on_request_sent)
        result = self._normalize_result(response)
        self._logger.info(
            "binance order.place ack | symbol=%s | status=%s | order_id=%s",
            result.symbol,
            result.status,
            result.order_id,
        )
        return result

    def cancel_order(
        self,
        *,
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> ExecutionOrderResult:
        params: dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = int(order_id)
        if client_order_id:
            params["origClientOrderId"] = client_order_id
        response = self._transport.request("order.cancel", params)
        return self._normalize_result(response)

    def query_order(
        self,
        *,
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> ExecutionOrderResult:
        params: dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = int(order_id)
        if client_order_id:
            params["origClientOrderId"] = client_order_id
        response = self._transport.request("order.status", params)
        return self._normalize_result(response)

    def close(self) -> None:
        self._transport.close()
        self._user_data_stream.close()
        self._logger.info("binance execution adapter closed")

    def diagnostics(self) -> dict[str, Any]:
        return {
            "route": self.ROUTE_NAME,
            "transport": self._transport.diagnostics(),
            "user_stream": self._user_data_stream.diagnostics(),
        }

    def _build_place_order_params(self, request: ExecutionOrderRequest) -> dict[str, Any]:
        params: dict[str, Any] = {
            "symbol": request.instrument_id.symbol,
            "side": request.side.upper(),
            "type": request.order_type.upper(),
            "newOrderRespType": request.response_type,
        }
        if request.quantity is not None:
            params["quantity"] = self._normalize_decimal(request.quantity)
        if request.price is not None:
            params["price"] = self._normalize_decimal(request.price)
        if request.time_in_force:
            params["timeInForce"] = request.time_in_force
        if request.position_side:
            params["positionSide"] = request.position_side
        if request.reduce_only is not None:
            params["reduceOnly"] = request.reduce_only
        if request.close_position is not None:
            params["closePosition"] = request.close_position
        if request.new_client_order_id:
            params["newClientOrderId"] = request.new_client_order_id
        if request.stop_price is not None:
            params["stopPrice"] = self._normalize_decimal(request.stop_price)
        if request.activation_price is not None:
            params["activationPrice"] = self._normalize_decimal(request.activation_price)
        if request.callback_rate is not None:
            params["callbackRate"] = self._normalize_decimal(request.callback_rate)
        if request.working_type:
            params["workingType"] = request.working_type
        if request.price_protect is not None:
            params["priceProtect"] = request.price_protect
        return params

    def _normalize_result(self, response: dict[str, Any]) -> ExecutionOrderResult:
        result = response.get("result", {})
        if not isinstance(result, dict):
            result = {}
        return ExecutionOrderResult(
            exchange="binance",
            route=self.ROUTE_NAME,
            request_id=str(response.get("id", "")),
            symbol=str(result.get("symbol", "")),
            order_id=self._str_or_none(result.get("orderId")),
            client_order_id=self._str_or_none(result.get("clientOrderId")),
            status=str(result.get("status", "")),
            side=str(result.get("side", "")),
            order_type=str(result.get("type", "")),
            position_side=self._str_or_none(result.get("positionSide")),
            price=self._str_or_none(result.get("price")),
            original_qty=self._str_or_none(result.get("origQty")),
            executed_qty=self._str_or_none(result.get("executedQty")),
            avg_price=self._str_or_none(result.get("avgPrice")),
            update_time=self._int_or_none(result.get("updateTime")),
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
