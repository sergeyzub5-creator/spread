from __future__ import annotations

import unittest
from decimal import Decimal

from app.core.execution.bybit_linear_adapter import BybitLinearExecutionAdapter
from app.core.execution.bybit_trade_ws import BybitTradeWebSocketError
from app.core.models.account import ExchangeCredentials
from app.core.models.execution import ExecutionOrderRequest
from app.core.models.instrument import InstrumentId, InstrumentKey, InstrumentRouting, InstrumentSpec


def _instrument() -> InstrumentId:
    return InstrumentId(
        key=InstrumentKey(exchange="bybit", market_type="linear_perp", symbol="XRPUSDT"),
        spec=InstrumentSpec(
            base_asset="XRP",
            quote_asset="USDT",
            contract_type="linear_perpetual",
            settle_asset="USDT",
            price_precision=Decimal("0.0001"),
            qty_precision=Decimal("1"),
            min_qty=Decimal("1"),
            min_notional=Decimal("5"),
        ),
        routing=InstrumentRouting(
            ws_channel="orderbook.1",
            ws_symbol="XRPUSDT",
            order_route="bybit_linear_trade_ws",
        ),
    )


class _FakeTransport:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def connect(self) -> bool:
        return True

    def request(self, _op: str, payload: dict, on_request_sent=None) -> dict:
        self.calls.append(dict(payload))
        if len(self.calls) == 1:
            raise BybitTradeWebSocketError("[10001] position idx not match position mode")
        return {
            "reqId": "2",
            "retCode": 0,
            "retMsg": "OK",
            "data": {
                "symbol": payload["symbol"],
                "orderId": "oid",
                "orderLinkId": payload.get("orderLinkId"),
                "orderStatus": "ACCEPTED",
                "side": payload["side"],
                "orderType": payload["orderType"],
                "qty": payload["qty"],
                "price": payload.get("price"),
            },
        }

    def close(self) -> None:
        return


class _FakePrivateStream:
    def on_execution_event(self, callback) -> None:
        return

    def connect(self) -> None:
        return

    def close(self) -> None:
        return


class BybitExecutionAdapterRetryTests(unittest.TestCase):
    def test_place_order_retries_with_inferred_position_idx_on_position_mode_error(self) -> None:
        adapter = BybitLinearExecutionAdapter(ExchangeCredentials(exchange="bybit", api_key="key", api_secret="secret"))
        adapter._transport = _FakeTransport()
        adapter._private_stream = _FakePrivateStream()

        request = ExecutionOrderRequest(
            instrument_id=_instrument(),
            side="BUY",
            order_type="LIMIT",
            quantity=Decimal("10"),
            price=Decimal("0.5"),
            time_in_force="GTC",
        )

        result = adapter.place_order(request)

        self.assertEqual(result.order_id, "oid")
        self.assertEqual(len(adapter._transport.calls), 2)
        self.assertNotIn("positionIdx", adapter._transport.calls[0])
        self.assertEqual(adapter._transport.calls[1].get("positionIdx"), 1)


if __name__ == "__main__":
    unittest.main()
