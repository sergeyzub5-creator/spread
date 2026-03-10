from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from decimal import Decimal
from typing import Any

from app.core.bitget.http_client import BitgetSignedHttpClient
from app.core.execution.adapter import ExecutionAdapter
from app.core.logging.logger_factory import get_logger
from app.core.models.account import ExchangeCredentials
from app.core.models.execution import ExecutionOrderRequest, ExecutionOrderResult, ExecutionStreamEvent


class BitgetLinearRestExecutionAdapter(ExecutionAdapter):
    ROUTE_NAME = "bitget_linear_rest_probe"
    PLACE_ORDER_PATH = "/api/v2/mix/order/place-order"
    CANCEL_ORDER_PATH = "/api/v2/mix/order/cancel-order"
    QUERY_ORDER_PATH = "/api/v2/mix/order/detail"
    PRODUCT_TYPE = "usdt-futures"

    def __init__(self, credentials: ExchangeCredentials) -> None:
        self._credentials = credentials
        self._client = BitgetSignedHttpClient(credentials)
        self._logger = get_logger("execution.bitget_linear_rest")

    def route_name(self) -> str:
        return self.ROUTE_NAME

    def connect(self) -> None:
        self._logger.info("bitget rest execution adapter connected")

    def on_execution_event(self, callback: Callable[[ExecutionStreamEvent], None]) -> None:
        del callback

    def place_order(
        self,
        request: ExecutionOrderRequest,
        on_request_sent: Callable[[dict[str, Any]], None] | None = None,
    ) -> ExecutionOrderResult:
        body = self._build_place_order_body(request)
        sent_at_ms = int(time.time() * 1000)
        if on_request_sent is not None:
            on_request_sent(
                {
                    "request_id": body["clientOid"],
                    "channel": "rest-place-order",
                    "sent_at_ms": sent_at_ms,
                    "connection_reused": True,
                }
            )
        response = self._client.post(self.PLACE_ORDER_PATH, body=body)
        response_at_ms = int(time.time() * 1000)
        data = response.get("data")
        payload = data if isinstance(data, dict) else {}
        raw = dict(response)
        raw["_transport_meta"] = {
            "request_id": body["clientOid"],
            "channel": "rest-place-order",
            "sent_at_ms": sent_at_ms,
            "response_at_ms": response_at_ms,
            "latency_ms": max(0, response_at_ms - sent_at_ms),
            "connection_reused": True,
        }
        result = ExecutionOrderResult(
            exchange="bitget",
            route=self.ROUTE_NAME,
            request_id=str(body["clientOid"]),
            symbol=str(body.get("symbol", "")).upper(),
            order_id=self._str_or_none(payload.get("orderId")),
            client_order_id=self._str_or_none(payload.get("clientOid") or body.get("clientOid")),
            status="ACCEPTED",
            side=str(body.get("side", "")),
            order_type=str(body.get("orderType", "")),
            position_side=self._str_or_none(body.get("tradeSide")),
            price=self._str_or_none(body.get("price")),
            original_qty=self._str_or_none(body.get("size")),
            executed_qty=None,
            avg_price=None,
            update_time=response_at_ms,
            raw=raw,
        )
        self._logger.info(
            "bitget rest place-order ack | symbol=%s | order_id=%s | latency_ms=%s",
            result.symbol,
            result.order_id,
            raw["_transport_meta"]["latency_ms"],
        )
        return result

    def cancel_order(
        self,
        *,
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> ExecutionOrderResult:
        body: dict[str, Any] = {
            "symbol": str(symbol).upper(),
            "productType": self.PRODUCT_TYPE,
        }
        if order_id is not None:
            body["orderId"] = str(order_id)
        if client_order_id:
            body["clientOid"] = client_order_id
        response = self._client.post(self.CANCEL_ORDER_PATH, body=body)
        return self._normalize_query_result(response)

    def query_order(
        self,
        *,
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> ExecutionOrderResult:
        params: dict[str, Any] = {
            "symbol": str(symbol).upper(),
            "productType": self.PRODUCT_TYPE,
        }
        if order_id is not None:
            params["orderId"] = str(order_id)
        if client_order_id:
            params["clientOid"] = client_order_id
        response = self._client.get(self.QUERY_ORDER_PATH, params=params)
        return self._normalize_query_result(response)

    def close(self) -> None:
        self._logger.info("bitget rest execution adapter closed")

    def diagnostics(self) -> dict[str, Any]:
        return {
            "route": self.ROUTE_NAME,
            "transport": {
                "connected": True,
                "mode": "rest",
            },
        }

    def _build_place_order_body(self, request: ExecutionOrderRequest) -> dict[str, Any]:
        order_type = str(request.order_type).strip().lower()
        body: dict[str, Any] = {
            "symbol": request.instrument_id.symbol,
            "productType": self.PRODUCT_TYPE,
            "marginCoin": request.instrument_id.spec.settle_asset or "USDT",
            "marginMode": "crossed",
            "side": str(request.side).strip().lower(),
            "orderType": order_type,
            "size": self._normalize_decimal(request.quantity),
            "clientOid": request.new_client_order_id or str(uuid.uuid4()),
        }
        if request.price is not None:
            body["price"] = self._normalize_decimal(request.price)
        if order_type == "limit":
            body["force"] = str(request.time_in_force or "gtc").strip().lower()
        if request.reduce_only is not None:
            body["reduceOnly"] = "YES" if bool(request.reduce_only) else "NO"
        if request.position_side:
            body["tradeSide"] = str(request.position_side).strip().lower()
        return body

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
