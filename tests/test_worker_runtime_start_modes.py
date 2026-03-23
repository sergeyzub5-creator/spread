from __future__ import annotations

import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from app.core.events.bus import EventBus
from app.core.market_data.service import MarketDataService
from app.core.models.account import ExchangeCredentials
from app.core.models.instrument import InstrumentId, InstrumentKey, InstrumentRouting, InstrumentSpec
from app.core.models.market_data import QuoteDepth20, QuoteDepthLevel, QuoteL1
from app.core.models.workers import StrategyPosition, StrategyState, WorkerTask
from app.core.workers.runtime_core import WorkerRuntime
from app.core.workers.runtime_state_guards import maybe_restore_in_position_state


def _make_linear_instrument(*, exchange: str, symbol: str) -> InstrumentId:
    normalized_exchange = str(exchange).strip().lower()
    normalized_symbol = str(symbol).strip().upper()
    return InstrumentId(
        key=InstrumentKey(exchange=normalized_exchange, market_type="linear_perp", symbol=normalized_symbol),
        spec=InstrumentSpec(
            base_asset=normalized_symbol.replace("USDT", ""),
            quote_asset="USDT",
            contract_type="linear_perpetual",
            settle_asset="USDT",
            price_precision=Decimal("0.0001"),
            qty_precision=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5"),
        ),
        routing=InstrumentRouting(
            ws_symbol=normalized_symbol,
            ws_channel="orderbook.1",
            order_route=f"{normalized_exchange}_linear_trade_ws",
        ),
    )


class _FakeMarketDataService(MarketDataService):
    def __init__(self) -> None:
        super().__init__()
        self.subscriptions: list[tuple[str, str, bool]] = []

    def subscribe_l1(self, instrument, callback, *, enable_depth20: bool = True) -> None:  # type: ignore[override]
        self.subscriptions.append((instrument.exchange, instrument.symbol, bool(enable_depth20)))

    def unsubscribe_l1(self, instrument, callback) -> None:  # type: ignore[override]
        return

    def ensure_depth20(self, instrument) -> None:  # type: ignore[override]
        return

    def release_depth20(self, instrument) -> None:  # type: ignore[override]
        return


class _FakeExecutionAdapter:
    def __init__(self, *, route_name: str, connected: bool, authenticated: bool | None) -> None:
        self._route_name = route_name
        self.connected = connected
        self.authenticated = authenticated

    def route_name(self) -> str:
        return self._route_name

    def diagnostics(self) -> dict:
        return {
            "route": self._route_name,
            "transport": {
                "connected": self.connected,
                "authenticated": self.authenticated,
                "reconnect_attempts_total": 0,
            },
        }

    def close(self) -> None:
        return


def _make_quote(instrument: InstrumentId, *, bid: str = "100", ask: str = "101", ts: int = 1) -> QuoteL1:
    return QuoteL1(
        instrument_id=instrument,
        bid=Decimal(bid),
        ask=Decimal(ask),
        bid_qty=Decimal("10"),
        ask_qty=Decimal("10"),
        ts_exchange=ts,
        ts_local=ts,
        source="public_ws",
    )


def _make_depth20(instrument: InstrumentId, *, ts: int = 1) -> QuoteDepth20:
    level = QuoteDepthLevel(price=Decimal("100"), quantity=Decimal("10"))
    return QuoteDepth20(
        instrument_id=instrument,
        bids=(level,),
        asks=(level,),
        ts_local=ts,
        source="public_ws",
    )


class WorkerRuntimeStartModeTests(unittest.TestCase):
    def test_execution_stream_status_requires_auth_when_present(self) -> None:
        status = WorkerRuntime._derive_execution_stream_status(
            {
                "left": {"connected": True, "authenticated": True},
                "right": {"connected": True, "authenticated": False},
            }
        )

        self.assertEqual(status, "DEGRADED")

    def test_spread_entry_mid_alarm_warms_dual_execution_on_start_without_single_adapter(self) -> None:
        left = _make_linear_instrument(exchange="binance", symbol="BTCUSDT")
        right = _make_linear_instrument(exchange="bybit", symbol="BTCUSDT")
        task = WorkerTask(
            worker_id="spread-start-mid-alarm",
            left_instrument=left,
            right_instrument=right,
            entry_threshold=Decimal("0.01"),
            exit_threshold=Decimal("0.00"),
            target_notional=Decimal("10"),
            step_notional=Decimal("10"),
            execution_mode="spread_entry_execution",
            run_mode="spread_entry_execution",
            execution_credentials=None,
            left_execution_credentials=ExchangeCredentials(exchange="binance", api_key="k1", api_secret="s1"),
            right_execution_credentials=ExchangeCredentials(exchange="bybit", api_key="k2", api_secret="s2"),
            runtime_params={"mid_alarm_enabled": "1", "mid_alarm_window_sec": "60"},
        )
        runtime = WorkerRuntime(task=task, market_data_service=_FakeMarketDataService(), event_bus=EventBus())

        with (
            patch.object(runtime, "_ensure_execution_adapter", side_effect=AssertionError("single adapter should not be used")),
            patch.object(runtime, "_ensure_dual_execution_adapters", return_value={}) as dual_adapters,
            patch.object(runtime, "_start_runtime_watchdog", return_value=None),
        ):
            runtime.start()

        self.assertEqual(runtime.state.status, "running")
        self.assertEqual(runtime.state.metrics.get("activity_status"), "STARTING")
        self.assertEqual(dual_adapters.call_count, 1)
        self.assertTrue(runtime._mid_alarm_resources_held)
        self.assertGreater(runtime._mid_alarm_armed_until_ms, 0)
        self.assertTrue(bool(runtime.state.metrics.get("mid_alarm_active")))

    def test_startup_entry_gate_waits_for_quotes_streams_and_depth(self) -> None:
        left = _make_linear_instrument(exchange="binance", symbol="BTCUSDT")
        right = _make_linear_instrument(exchange="bybit", symbol="BTCUSDT")
        task = WorkerTask(
            worker_id="spread-startup-gate",
            left_instrument=left,
            right_instrument=right,
            entry_threshold=Decimal("0.01"),
            exit_threshold=Decimal("0.00"),
            target_notional=Decimal("10"),
            step_notional=Decimal("10"),
            execution_mode="spread_entry_execution",
            run_mode="spread_entry_execution",
            execution_credentials=None,
            left_execution_credentials=ExchangeCredentials(exchange="binance", api_key="k1", api_secret="s1"),
            right_execution_credentials=ExchangeCredentials(exchange="bybit", api_key="k2", api_secret="s2"),
            runtime_params={"mid_alarm_enabled": "1", "mid_alarm_window_sec": "60"},
        )
        market_data = _FakeMarketDataService()
        runtime = WorkerRuntime(task=task, market_data_service=market_data, event_bus=EventBus())
        left_adapter = _FakeExecutionAdapter(route_name="binance", connected=False, authenticated=True)
        right_adapter = _FakeExecutionAdapter(route_name="bybit", connected=False, authenticated=True)

        def _install_dual_adapters() -> dict:
            runtime._left_execution_adapter = left_adapter
            runtime._right_execution_adapter = right_adapter
            return {"left": left_adapter, "right": right_adapter}

        with (
            patch.object(runtime, "_ensure_execution_adapter", side_effect=AssertionError("single adapter should not be used")),
            patch.object(runtime, "_ensure_dual_execution_adapters", side_effect=_install_dual_adapters),
            patch.object(runtime, "_start_runtime_watchdog", return_value=None),
        ):
            runtime.start()

        self.assertEqual(runtime._startup_entry_gate_block_reason(), "STARTUP_WAIT_QUOTES")

        runtime._latest_quotes[left] = _make_quote(left, ts=10)
        runtime._latest_quotes[right] = _make_quote(right, ts=11)
        runtime._publish_state(force=True)
        self.assertEqual(runtime._startup_entry_gate_block_reason(), "STARTUP_WAIT_EXECUTION_STREAMS")

        left_adapter.connected = True
        right_adapter.connected = True
        runtime._publish_state(force=True)
        self.assertEqual(runtime._startup_entry_gate_block_reason(), "STARTUP_WAIT_DEPTH")

        market_data._depth20_cache[left] = _make_depth20(left, ts=12)
        market_data._depth20_cache[right] = _make_depth20(right, ts=13)
        runtime._publish_state(force=True)
        self.assertIsNone(runtime._startup_entry_gate_block_reason())
        self.assertTrue(runtime._startup_entry_gate_opened)
        self.assertTrue(bool(runtime.state.metrics.get("startup_entry_ready")))
        runtime._update_activity_status()
        self.assertEqual(runtime.state.metrics.get("activity_status"), "WAITING_ENTRY")

    def test_startup_gate_does_not_consume_forced_signal_while_blocked(self) -> None:
        left = _make_linear_instrument(exchange="binance", symbol="BTCUSDT")
        right = _make_linear_instrument(exchange="bybit", symbol="BTCUSDT")
        task = WorkerTask(
            worker_id="spread-startup-gate-forced",
            left_instrument=left,
            right_instrument=right,
            entry_threshold=Decimal("0.01"),
            exit_threshold=Decimal("0.00"),
            target_notional=Decimal("10"),
            step_notional=Decimal("10"),
            execution_mode="spread_entry_execution",
            run_mode="spread_entry_execution",
            execution_credentials=None,
            left_execution_credentials=ExchangeCredentials(exchange="binance", api_key="k1", api_secret="s1"),
            right_execution_credentials=ExchangeCredentials(exchange="bybit", api_key="k2", api_secret="s2"),
            runtime_params={"mid_alarm_enabled": "1", "mid_alarm_window_sec": "60"},
        )
        runtime = WorkerRuntime(task=task, market_data_service=_FakeMarketDataService(), event_bus=EventBus())
        with patch.object(runtime, "_start_runtime_watchdog", return_value=None), patch.object(runtime, "_ensure_dual_execution_adapters", return_value={}):
            runtime.start()
        runtime._forced_entry_signal_requested = True
        runtime._latest_quotes[left] = _make_quote(left, ts=20)
        runtime._latest_quotes[right] = _make_quote(right, ts=21)
        decision = runtime._build_entry_decision()
        self.assertIsNotNone(decision)
        self.assertEqual(decision.block_reason, "STARTUP_WAIT_EXECUTION")
        self.assertTrue(runtime._forced_entry_signal_requested)

    def test_on_quote_does_not_trigger_order_actions_while_startup_gate_blocked(self) -> None:
        left = _make_linear_instrument(exchange="binance", symbol="BTCUSDT")
        right = _make_linear_instrument(exchange="bybit", symbol="BTCUSDT")
        task = WorkerTask(
            worker_id="spread-startup-gate-on-quote",
            left_instrument=left,
            right_instrument=right,
            entry_threshold=Decimal("0.01"),
            exit_threshold=Decimal("0.00"),
            target_notional=Decimal("10"),
            step_notional=Decimal("10"),
            execution_mode="spread_entry_execution",
            run_mode="spread_entry_execution",
            execution_credentials=None,
            left_execution_credentials=ExchangeCredentials(exchange="binance", api_key="k1", api_secret="s1"),
            right_execution_credentials=ExchangeCredentials(exchange="bybit", api_key="k2", api_secret="s2"),
            runtime_params={"mid_alarm_enabled": "1", "mid_alarm_window_sec": "60"},
        )
        runtime = WorkerRuntime(task=task, market_data_service=_FakeMarketDataService(), event_bus=EventBus())
        left_adapter = _FakeExecutionAdapter(route_name="binance", connected=False, authenticated=True)
        right_adapter = _FakeExecutionAdapter(route_name="bybit", connected=False, authenticated=True)

        def _install_dual_adapters() -> dict:
            runtime._left_execution_adapter = left_adapter
            runtime._right_execution_adapter = right_adapter
            return {"left": left_adapter, "right": right_adapter}

        with (
            patch.object(runtime, "_ensure_dual_execution_adapters", side_effect=_install_dual_adapters),
            patch.object(runtime, "_start_runtime_watchdog", return_value=None),
        ):
            runtime.start()

        with (
            patch.object(runtime, "_evaluate_spread_entry") as eval_entry,
            patch.object(runtime, "_evaluate_spread_exit") as eval_exit,
            patch.object(runtime, "_reevaluate_active_spread_execution") as reevaluate_exec,
            patch.object(runtime, "_request_hedge_protection_check") as hedge_check,
        ):
            runtime.on_quote(_make_quote(left, ts=1))
            runtime.on_quote(_make_quote(right, ts=2))

        eval_entry.assert_not_called()
        eval_exit.assert_not_called()
        reevaluate_exec.assert_not_called()
        hedge_check.assert_not_called()

    def test_on_quote_does_not_trigger_exit_or_hedge_until_execution_streams_ready(self) -> None:
        left = _make_linear_instrument(exchange="binance", symbol="BTCUSDT")
        right = _make_linear_instrument(exchange="bybit", symbol="BTCUSDT")
        task = WorkerTask(
            worker_id="spread-exec-readiness-gate",
            left_instrument=left,
            right_instrument=right,
            entry_threshold=Decimal("0.01"),
            exit_threshold=Decimal("0.00"),
            target_notional=Decimal("10"),
            step_notional=Decimal("10"),
            execution_mode="spread_entry_execution",
            run_mode="spread_entry_execution",
            execution_credentials=None,
            left_execution_credentials=ExchangeCredentials(exchange="binance", api_key="k1", api_secret="s1"),
            right_execution_credentials=ExchangeCredentials(exchange="bybit", api_key="k2", api_secret="s2"),
            runtime_params={"mid_alarm_enabled": "1", "mid_alarm_window_sec": "60"},
        )
        market_data = _FakeMarketDataService()
        runtime = WorkerRuntime(task=task, market_data_service=market_data, event_bus=EventBus())
        left_adapter = _FakeExecutionAdapter(route_name="binance", connected=False, authenticated=True)
        right_adapter = _FakeExecutionAdapter(route_name="bybit", connected=False, authenticated=True)

        def _install_dual_adapters() -> dict:
            runtime._left_execution_adapter = left_adapter
            runtime._right_execution_adapter = right_adapter
            return {"left": left_adapter, "right": right_adapter}

        with (
            patch.object(runtime, "_ensure_dual_execution_adapters", side_effect=_install_dual_adapters),
            patch.object(runtime, "_start_runtime_watchdog", return_value=None),
        ):
            runtime.start()

        runtime._latest_quotes[left] = _make_quote(left, ts=10)
        runtime._latest_quotes[right] = _make_quote(right, ts=11)
        market_data._depth20_cache[left] = _make_depth20(left, ts=12)
        market_data._depth20_cache[right] = _make_depth20(right, ts=13)
        runtime._startup_entry_gate_opened = True
        runtime.position = StrategyPosition(
            direction="LONG_SPREAD",
            entry_edge=Decimal("0.01"),
            active_edge="edge_1",
            left_side="SELL",
            right_side="BUY",
            left_target_qty=Decimal("1"),
            right_target_qty=Decimal("1"),
            left_filled_qty=Decimal("1"),
            right_filled_qty=Decimal("1"),
            left_avg_fill_price=Decimal("100"),
            right_avg_fill_price=Decimal("99"),
            entry_time=1,
            state=StrategyState.IN_POSITION,
        )
        runtime.strategy_state = StrategyState.IN_POSITION

        with (
            patch.object(runtime, "_evaluate_spread_exit") as eval_exit,
            patch.object(runtime, "_reevaluate_active_spread_execution") as reevaluate_exec,
            patch.object(runtime, "_request_hedge_protection_check") as hedge_check,
        ):
            runtime.on_quote(_make_quote(left, ts=20))

        eval_exit.assert_not_called()
        reevaluate_exec.assert_not_called()
        hedge_check.assert_not_called()

    def test_restored_same_pair_runtime_uses_new_target_notional(self) -> None:
        left = _make_linear_instrument(exchange="binance", symbol="BTCUSDT")
        right = _make_linear_instrument(exchange="bybit", symbol="BTCUSDT")
        task = WorkerTask(
            worker_id="spread-restore-new-target",
            left_instrument=left,
            right_instrument=right,
            entry_threshold=Decimal("0.01"),
            exit_threshold=Decimal("0.00"),
            target_notional=Decimal("15"),
            step_notional=Decimal("15"),
            execution_mode="spread_entry_execution",
            run_mode="spread_entry_execution",
            execution_credentials=None,
            left_execution_credentials=ExchangeCredentials(exchange="binance", api_key="k1", api_secret="s1"),
            right_execution_credentials=ExchangeCredentials(exchange="bybit", api_key="k2", api_secret="s2"),
            runtime_params={"entry_notional_usdt": "15", "cycle_count": "1"},
        )
        runtime = WorkerRuntime(task=task, market_data_service=_FakeMarketDataService(), event_bus=EventBus())
        runtime._latest_quotes[left] = _make_quote(left, bid="100", ask="100", ts=10)
        runtime._latest_quotes[right] = _make_quote(right, bid="101", ask="101", ts=11)
        runtime.left_leg_state.side = "SELL"
        runtime.right_leg_state.side = "BUY"
        runtime.left_leg_state.filled_qty = Decimal("0.09")
        runtime.right_leg_state.filled_qty = Decimal("0.09")
        runtime.left_leg_state.actual_position_qty = Decimal("0.09")
        runtime.right_leg_state.actual_position_qty = Decimal("0.09")
        runtime._entry_growth_limited = True
        runtime._entry_growth_limit_reason = "MARGIN_LIMIT_REACHED"
        runtime._entry_growth_limit_notional_usdt = Decimal("10")
        runtime._entry_growth_limit_qty = Decimal("0.09")
        runtime.state.metrics["entry_growth_limited"] = True
        runtime.state.metrics["entry_growth_limit_reason"] = "MARGIN_LIMIT_REACHED"
        runtime.state.metrics["entry_growth_limit_notional_usdt"] = "10"
        runtime.state.metrics["entry_growth_limit_qty"] = "0.09"

        restored = maybe_restore_in_position_state(runtime)

        self.assertTrue(restored)
        self.assertFalse(runtime._entry_growth_limited)
        self.assertIsNone(runtime._entry_growth_limit_notional_usdt)
        next_notional = runtime._effective_entry_cycle_notional_usdt(
            SimpleNamespace(left_action="SELL", right_action="BUY")
        )
        self.assertGreater(next_notional, Decimal("0"))


if __name__ == "__main__":
    unittest.main()
