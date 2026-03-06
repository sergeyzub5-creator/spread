from __future__ import annotations

import uuid
from collections.abc import Callable
from decimal import Decimal
from typing import Any

from app.core.bitget.http_client import BitgetSignedHttpClient
from app.core.execution.adapter import ExecutionAdapter
from app.core.execution.bitget_linear_private_stream import BitgetLinearPrivateExecutionStream
from app.core.execution.bitget_linear_trade_ws import BitgetLinearTradeWebSocketTransport
from app.core.logging.logger_factory import get_logger
from app.core.models.account import ExchangeCredentials
from app.core.models.execution import ExecutionOrderRequest, ExecutionOrderResult, ExecutionStreamEvent


class BitgetLinearExecutionAdapter(ExecutionAdapter):
    ROUTE_NAME = "bitget_linear_trade_ws"
    QUERY_ORDER_PATH = "/api/v2/mix/order/detail"
    PRODUCT_TYPE = "USDT-FUTURES"

    def __init__(self, credentials: ExchangeCredentials) -> None:
        self._credentials = credentials
        self._logger = get_logger("execution.bitget_linear")
        self._transport = BitgetLinearTradeWebSocketTransport(credentials)
        self._private_stream = BitgetLinearPrivateExecutionStream(credentials)
        self._client = BitgetSignedHttpClient(credentials)
        self._account_profile = dict(getattr(credentials, "account_profile", {}) or {})

    def route_name(self) -> str:
        return self.ROUTE_NAME

    def connect(self) -> None:
        self._ensure_supported_account_mode()
        self._transport.connect()
        self._logger.info(
            "bitget execution adapter connected | route=%s | execution_stack=%s",
            self.ROUTE_NAME,
            self._account_profile.get("execution_stack"),
        )

    def on_execution_event(self, callback: Callable[[ExecutionStreamEvent], None]) -> None:
        self._private_stream.on_execution_event(callback)
        self._private_stream.connect()
        self._logger.info("bitget execution adapter private stream attached")

    def place_order(
        self,
        request: ExecutionOrderRequest,
        on_request_sent: Callable[[dict[str, Any]], None] | None = None,
    ) -> ExecutionOrderResult:
        self._assert_route(request.instrument_id.routing.order_route)
        payload = self._build_place_order_payload(request)
        response = self._transport.request("place-order", payload, on_request_sent=on_request_sent)
        result = self._normalize_trade_result(response, request=request)
        self._logger.info(
            "bitget place-order ack | symbol=%s | order_id=%s | status=%s",
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
        args: dict[str, Any] = {
            "instType": self.PRODUCT_TYPE,
            "instId": str(symbol).upper(),
            "channel": "cancel-order",
            "params": {},
        }
        if order_id is not None:
            args["params"]["orderId"] = str(order_id)
        if client_order_id:
            args["params"]["clientOid"] = client_order_id
        response = self._transport.request("cancel-order", args)
        return self._normalize_trade_result(response, request=None)

    def query_order(
        self,
        *,
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> ExecutionOrderResult:
        params: dict[str, Any] = {
            "symbol": str(symbol).upper(),
            "productType": self.PRODUCT_TYPE.lower(),
        }
        if order_id is not None:
            params["orderId"] = str(order_id)
        if client_order_id:
            params["clientOid"] = client_order_id
        response = self._client.get(self.QUERY_ORDER_PATH, params=params)
        return self._normalize_query_result(response)

    def close(self) -> None:
        self._transport.close()
        self._private_stream.close()
        self._logger.info("bitget execution adapter closed")

    def _build_place_order_payload(self, request: ExecutionOrderRequest) -> dict[str, Any]:
        order_type = str(request.order_type).strip().upper()
        params: dict[str, Any] = {
            "orderType": order_type.lower(),
            "side": str(request.side).strip().lower(),
            "size": self._normalize_decimal(request.quantity),
            "marginCoin": request.instrument_id.spec.settle_asset or "USDT",
            "marginMode": "crossed",
            "clientOid": request.new_client_order_id or str(uuid.uuid4()),
        }
        if request.price is not None:
            params["price"] = self._normalize_decimal(request.price)
        if order_type == "LIMIT":
            params["force"] = str(request.time_in_force or "gtc").strip().lower()
        if request.reduce_only is not None:
            params["reduceOnly"] = "YES" if bool(request.reduce_only) else "NO"
        if request.position_side:
            params["tradeSide"] = str(request.position_side).strip().lower()

        return {
            "instType": self.PRODUCT_TYPE,
            "instId": request.instrument_id.symbol,
            "channel": "place-order",
            "params": params,
        }

    def _normalize_trade_result(
        self,
        response: dict[str, Any],
        request: ExecutionOrderRequest | None,
    ) -> ExecutionOrderResult:
        arg = response.get("arg")
        if isinstance(arg, dict):
            item = arg
        elif isinstance(arg, list) and arg and isinstance(arg[0], dict):
            item = arg[0]
        else:
            item = {}
        params = item.get("params", {}) if isinstance(item.get("params"), dict) else {}
        symbol = str(item.get("instId") or (request.instrument_id.symbol if request is not None else ""))
        return ExecutionOrderResult(
            exchange="bitget",
            route=self.ROUTE_NAME,
            request_id=str(self._extract_request_id(response) or ""),
            symbol=symbol,
            order_id=self._str_or_none(params.get("orderId")),
            client_order_id=self._str_or_none(params.get("clientOid")),
            status="ACCEPTED" if str(response.get("code", "")).strip() in {"0", "00000"} else "ERROR",
            side=str(params.get("side") or (request.side.lower() if request is not None else "")),
            order_type=str(params.get("orderType") or (request.order_type.lower() if request is not None else "")),
            position_side=self._str_or_none(params.get("tradeSide")),
            price=self._str_or_none(params.get("price")),
            original_qty=self._str_or_none(params.get("size")),
            executed_qty=None,
            avg_price=None,
            update_time=self._int_or_none(response.get("ts")),
            raw=response,
        )

    def _normalize_query_result(self, response: dict[str, Any]) -> ExecutionOrderResult:
        raw_data = response.get("data")
        data = raw_data[0] if isinstance(raw_data, list) and raw_data and isinstance(raw_data[0], dict) else {}
        if not data and isinstance(raw_data, dict):
            data = raw_data
        return ExecutionOrderResult(
            exchange="bitget",
            route=self.ROUTE_NAME,
            request_id="",
            symbol=str(data.get("symbol", "")).upper(),
            order_id=self._str_or_none(data.get("orderId")),
            client_order_id=self._str_or_none(data.get("clientOid")),
            status=str(data.get("state") or data.get("orderStatus") or ""),
            side=str(data.get("side", "")),
            order_type=str(data.get("orderType", "")),
            position_side=self._str_or_none(data.get("tradeSide") or data.get("holdSide")),
            price=self._str_or_none(data.get("price")),
            original_qty=self._str_or_none(data.get("size") or data.get("qty")),
            executed_qty=self._str_or_none(data.get("baseVolume") or data.get("cumExecQty")),
            avg_price=self._str_or_none(data.get("priceAvg") or data.get("avgPrice")),
            update_time=self._int_or_none(data.get("uTime") or data.get("updatedTime")),
            raw=response,
        )

    def _ensure_supported_account_mode(self) -> None:
        execution_stack = str(self._account_profile.get("execution_stack") or "").strip().lower()
        if execution_stack and execution_stack != "classic_v2_private_ws":
            raise RuntimeError(
                f"Bitget execution stack '{execution_stack}' is not supported by the current adapter"
            )

    @staticmethod
    def _extract_request_id(response: dict[str, Any]) -> str | None:
        direct_id = response.get("id")
        if direct_id:
            return str(direct_id)
        arg = response.get("arg")
        if isinstance(arg, dict):
            value = arg.get("id")
            if value:
                return str(value)
        if isinstance(arg, list) and arg and isinstance(arg[0], dict):
            value = arg[0].get("id")
            if value:
                return str(value)
        return None

    @staticmethod
    def _normalize_decimal(value: Decimal | None) -> str:
        if value is None:
            return ""
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
