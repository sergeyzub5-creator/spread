from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import replace
import json
import threading
import time
from decimal import Decimal
from typing import Any

from app.core.market_data.connector import PublicMarketDataConnector
from app.core.market_data.normalizer import QuoteNormalizer
from app.core.logging.logger_factory import get_logger
from app.core.models.instrument import InstrumentId
from app.core.models.market_data import QuoteDepth20, QuoteDepthLevel, QuoteL1

try:
    import websocket
except ImportError:  # pragma: no cover
    websocket = None


class _BinanceDepth20Worker:
    def __init__(self, *, instrument: InstrumentId, on_snapshot: Callable[[QuoteDepth20], None]) -> None:
        self._instrument = instrument
        self._on_snapshot = on_snapshot
        self._logger = get_logger("market_data.binance_depth20")
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._running = False
        self._ws_app = None

    def start(self) -> None:
        if websocket is None:
            self._logger.warning("binance depth20 worker skipped | reason=websocket_client_missing")
            return
        with self._lock:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(target=self._run_forever, name=f"binance-depth20-{self._instrument.symbol}", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            self._running = False
            ws_app = self._ws_app
        if ws_app is not None:
            try:
                ws_app.close()
            except Exception:
                pass

    def _stream_url(self) -> str:
        symbol = str(self._instrument.routing.ws_symbol or self._instrument.symbol).strip().lower()
        stream_name = f"{symbol}@depth20@100ms"
        if self._instrument.market_type == "linear_perp":
            return f"wss://fstream.binance.com/ws/{stream_name}"
        return f"wss://stream.binance.com:9443/ws/{stream_name}"

    def _run_forever(self) -> None:
        backoff_seconds = 1.0
        while True:
            with self._lock:
                if not self._running:
                    return
            ws_app = websocket.WebSocketApp(
                self._stream_url(),
                on_message=lambda _ws, message: self._on_message(message),
                on_error=lambda _ws, error: self._on_error(error),
                on_close=lambda _ws, code, msg: self._on_close(code, msg),
            )
            with self._lock:
                self._ws_app = ws_app
            try:
                ws_app.run_forever()
            except Exception as exc:  # pragma: no cover
                self._logger.error("binance depth20 ws loop crashed | symbol=%s | error=%s", self._instrument.symbol, exc)
            with self._lock:
                if not self._running:
                    return
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2.0, 10.0)

    def _on_message(self, message: str) -> None:
        try:
            payload = json.loads(message)
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        raw_bids = payload.get("bids")
        raw_asks = payload.get("asks")
        if not isinstance(raw_bids, list) or not isinstance(raw_asks, list):
            raw_bids = payload.get("b")
            raw_asks = payload.get("a")
        if not isinstance(raw_bids, list) or not isinstance(raw_asks, list):
            return
        bids = self._normalize_levels(raw_bids)
        asks = self._normalize_levels(raw_asks)
        source_symbol = str(payload.get("s") or payload.get("symbol") or self._instrument.routing.ws_symbol or self._instrument.symbol).strip()
        snapshot = QuoteDepth20(
            instrument_id=self._instrument,
            bids=tuple(bids[:20]),
            asks=tuple(asks[:20]),
            ts_local=int(time.time() * 1000),
            source="public_ws",
            source_symbol=source_symbol,
        )
        self._on_snapshot(snapshot)

    def _normalize_levels(self, levels: list[Any]) -> list[QuoteDepthLevel]:
        normalized: list[QuoteDepthLevel] = []
        for item in levels:
            if not isinstance(item, list) or len(item) < 2:
                continue
            try:
                price = Decimal(str(item[0]))
                qty = Decimal(str(item[1]))
            except Exception:
                continue
            if price <= Decimal("0") or qty <= Decimal("0"):
                continue
            normalized.append(QuoteDepthLevel(price=price, quantity=qty))
        return normalized

    def _on_error(self, error: Any) -> None:
        self._logger.warning("binance depth20 ws error | symbol=%s | error=%s", self._instrument.symbol, error)

    def _on_close(self, code: Any, msg: Any) -> None:
        self._logger.info("binance depth20 ws closed | symbol=%s | code=%s | message=%s", self._instrument.symbol, code, msg)


class _BitgetDepthWorker:
    STALE_STREAM_TIMEOUT_MS = 60000
    WATCHDOG_INTERVAL_SECONDS = 5.0
    PING_INTERVAL_SECONDS = 15.0

    def __init__(self, *, instrument: InstrumentId, on_snapshot: Callable[[QuoteDepth20], None]) -> None:
        self._instrument = instrument
        self._on_snapshot = on_snapshot
        self._logger = get_logger("market_data.bitget_depth")
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._ping_thread: threading.Thread | None = None
        self._running = False
        self._connected = False
        self._last_message_ts_ms = 0
        self._ws_app = None

    def start(self) -> None:
        if websocket is None:
            self._logger.warning("bitget depth worker skipped | reason=websocket_client_missing")
            return
        with self._lock:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(target=self._run_forever, name=f"bitget-depth-{self._instrument.symbol}", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            self._running = False
            self._connected = False
            ws_app = self._ws_app
        if ws_app is not None:
            try:
                ws_app.close()
            except Exception:
                pass

    def _run_forever(self) -> None:
        backoff_seconds = 1.0
        while True:
            with self._lock:
                if not self._running:
                    return
            ws_app = websocket.WebSocketApp(
                "wss://ws.bitget.com/v2/ws/public",
                on_open=lambda _ws: self._on_open(),
                on_message=lambda _ws, message: self._on_message(message),
                on_error=lambda _ws, error: self._on_error(error),
                on_close=lambda _ws, code, msg: self._on_close(code, msg),
            )
            with self._lock:
                self._ws_app = ws_app
            try:
                ws_app.run_forever()
            except Exception as exc:  # pragma: no cover
                self._logger.error("bitget depth ws loop crashed | symbol=%s | error=%s", self._instrument.symbol, exc)
            with self._lock:
                if not self._running:
                    return
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2.0, 10.0)

    def _subscription_arg(self) -> dict[str, str]:
        inst_type = "USDT-FUTURES" if self._instrument.market_type == "linear_perp" else "SPOT"
        return {
            "instType": inst_type,
            "channel": "books50",
            "instId": self._instrument.routing.ws_symbol,
        }

    def _on_open(self) -> None:
        ws_app = self._ws_app
        if ws_app is None:
            return
        self._connected = True
        self._last_message_ts_ms = int(time.time() * 1000)
        self._start_ping_loop()
        try:
            ws_app.send(json.dumps({"op": "subscribe", "args": [self._subscription_arg()]}))
        except Exception as exc:
            self._logger.warning("bitget depth subscribe failed | symbol=%s | error=%s", self._instrument.symbol, exc)

    def _on_message(self, message: str) -> None:
        raw_message = str(message).strip()
        if raw_message.lower() == "pong":
            self._last_message_ts_ms = int(time.time() * 1000)
            return
        try:
            payload = json.loads(raw_message)
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        if self._is_pong_payload(payload):
            self._last_message_ts_ms = int(time.time() * 1000)
            return
        # Treat any valid control/data payload as a sign of liveness.
        self._last_message_ts_ms = int(time.time() * 1000)
        arg = payload.get("arg", {}) if isinstance(payload.get("arg"), dict) else {}
        channel = str(arg.get("channel", "")).strip().lower()
        if channel != "books50":
            return
        source_symbol = str(arg.get("instId") or self._instrument.routing.ws_symbol or self._instrument.symbol).strip()
        data = payload.get("data")
        if not isinstance(data, list) or not data:
            return
        book = data[0] if isinstance(data[0], dict) else {}
        raw_bids = book.get("bids")
        raw_asks = book.get("asks")
        if not isinstance(raw_bids, list) or not isinstance(raw_asks, list):
            raw_bids = book.get("b")
            raw_asks = book.get("a")
        if not isinstance(raw_bids, list) or not isinstance(raw_asks, list):
            return
        bids = self._normalize_levels(raw_bids)
        asks = self._normalize_levels(raw_asks)
        snapshot = QuoteDepth20(
            instrument_id=self._instrument,
            bids=tuple(bids),
            asks=tuple(asks),
            ts_local=int(time.time() * 1000),
            source="public_ws",
            source_symbol=source_symbol,
        )
        self._last_message_ts_ms = int(snapshot.ts_local)
        self._on_snapshot(snapshot)

    @staticmethod
    def _is_pong_payload(payload: dict[str, Any]) -> bool:
        op = str(payload.get("op", "")).strip().lower()
        event = str(payload.get("event", "")).strip().lower()
        action = str(payload.get("action", "")).strip().lower()
        message = str(payload.get("msg", "")).strip().lower()
        ret_msg = str(payload.get("ret_msg", "")).strip().lower()
        return "pong" in {op, event, action, message, ret_msg}

    def _normalize_levels(self, levels: list[Any]) -> list[QuoteDepthLevel]:
        normalized: list[QuoteDepthLevel] = []
        for item in levels:
            if not isinstance(item, list) or len(item) < 2:
                continue
            try:
                price = Decimal(str(item[0]))
                qty = Decimal(str(item[1]))
            except Exception:
                continue
            if price <= Decimal("0") or qty <= Decimal("0"):
                continue
            normalized.append(QuoteDepthLevel(price=price, quantity=qty))
        return normalized

    def _on_error(self, error: Any) -> None:
        self._logger.warning("bitget depth ws error | symbol=%s | error=%s", self._instrument.symbol, error)

    def _on_close(self, code: Any, msg: Any) -> None:
        self._connected = False
        self._logger.info("bitget depth ws closed | symbol=%s | code=%s | message=%s", self._instrument.symbol, code, msg)

    def _start_ping_loop(self) -> None:
        with self._lock:
            if self._ping_thread is not None and self._ping_thread.is_alive():
                return
            self._ping_thread = threading.Thread(target=self._ping_loop, name=f"bitget-depth-ping-{self._instrument.symbol}", daemon=True)
            self._ping_thread.start()

    def _ping_loop(self) -> None:
        ping_elapsed_seconds = 0.0
        while True:
            time.sleep(self.WATCHDOG_INTERVAL_SECONDS)
            with self._lock:
                running = self._running
                connected = self._connected
                ws_app = self._ws_app
                last_message_ts_ms = int(self._last_message_ts_ms or 0)
            if not running or not connected:
                return
            now_ms = int(time.time() * 1000)
            if last_message_ts_ms > 0 and (now_ms - last_message_ts_ms) >= self.STALE_STREAM_TIMEOUT_MS:
                self._logger.warning(
                    "bitget depth ws stale stream detected | symbol=%s | silence_ms=%s | action=restart",
                    self._instrument.symbol,
                    now_ms - last_message_ts_ms,
                )
                with self._lock:
                    self._connected = False
                try:
                    if ws_app is not None:
                        ws_app.close()
                except Exception:
                    pass
                return
            ping_elapsed_seconds += self.WATCHDOG_INTERVAL_SECONDS
            if ping_elapsed_seconds < self.PING_INTERVAL_SECONDS:
                continue
            ping_elapsed_seconds = 0.0
            if ws_app is not None:
                try:
                    ws_app.send("ping")
                except Exception:
                    with self._lock:
                        self._connected = False
                    try:
                        ws_app.close()
                    except Exception:
                        pass
                    return


class MarketDataService:
    """Owns public WS subscriptions and normalized quote fan-out.

    This service is transport-only. It must not hold strategy state, spread state,
    or a shared business cache used by worker logic.
    """

    def __init__(self) -> None:
        self._logger = get_logger("market_data.service")
        self._subscribers: dict[InstrumentId, list[Callable[[QuoteL1], None]]] = defaultdict(list)
        self._connectors: dict[str, PublicMarketDataConnector] = {}
        self._normalizers: dict[str, QuoteNormalizer] = {}
        self._depth20_cache: dict[InstrumentId, QuoteDepth20] = {}
        self._depth20_workers: dict[InstrumentId, _BinanceDepth20Worker | _BitgetDepthWorker] = {}
        self._depth20_seq: dict[InstrumentId, int] = {}
        self._depth20_diagnostics: dict[InstrumentId, dict[str, Any]] = {}
        self._lock = threading.RLock()

    def register_exchange_transport(
        self,
        transport_key: str,
        connector: PublicMarketDataConnector,
        normalizer: QuoteNormalizer,
    ) -> None:
        with self._lock:
            self._connectors[transport_key] = connector
            self._normalizers[transport_key] = normalizer
        connector.on_quote(lambda event, key=transport_key: self._handle_raw_quote(key, event))

    def subscribe_l1(self, instrument: InstrumentId, callback: Callable[[QuoteL1], None]) -> None:
        with self._lock:
            callbacks = self._subscribers[instrument]
            first_subscription = not callbacks
            callbacks.append(callback)
            connector = self._connectors.get(self._transport_key(instrument))
        if connector is not None and first_subscription:
            connector.connect()
            connector.subscribe_l1(instrument)
            self._start_depth20_worker_if_supported(instrument)

    def unsubscribe_l1(self, instrument: InstrumentId, callback: Callable[[QuoteL1], None]) -> None:
        connector: PublicMarketDataConnector | None = None
        should_unsubscribe = False
        with self._lock:
            callbacks = self._subscribers.get(instrument, [])
            if callback in callbacks:
                callbacks.remove(callback)
            if not callbacks and instrument in self._subscribers:
                self._subscribers.pop(instrument, None)
                connector = self._connectors.get(self._transport_key(instrument))
                should_unsubscribe = connector is not None
        if should_unsubscribe and connector is not None:
            connector.unsubscribe_l1(instrument)
            self._stop_depth20_worker(instrument)

    def publish_quote(self, quote: QuoteL1) -> None:
        with self._lock:
            callbacks = list(self._subscribers.get(quote.instrument_id, []))
        for callback in callbacks:
            try:
                callback(quote)
            except Exception as exc:
                self._logger.error("quote subscriber failed | instrument=%s | error=%s", quote.instrument_id.symbol, exc)

    def shutdown(self) -> None:
        with self._lock:
            connectors = list(self._connectors.values())
            depth_workers = list(self._depth20_workers.values())
            self._subscribers.clear()
            self._connectors.clear()
            self._normalizers.clear()
            self._depth20_cache.clear()
            self._depth20_workers.clear()
            self._depth20_seq.clear()
            self._depth20_diagnostics.clear()
        for worker in depth_workers:
            try:
                worker.stop()
            except Exception:
                pass
        for connector in connectors:
            try:
                connector.close()
            except Exception:
                pass

    def get_depth20_snapshot(self, instrument: InstrumentId) -> QuoteDepth20 | None:
        with self._lock:
            return self._depth20_cache.get(instrument)

    def get_depth20_diagnostics(self, instrument: InstrumentId) -> dict[str, Any]:
        with self._lock:
            raw = self._depth20_diagnostics.get(instrument)
            if isinstance(raw, dict):
                return dict(raw)
        return {
            "depth_updates_count": 0,
            "depth_reject_count": 0,
            "depth_last_reason": "NO_DATA",
            "last_depth_ts_ms": None,
        }

    def _handle_raw_quote(self, exchange: str, event: object) -> None:
        if not isinstance(event, dict):
            return
        instrument = event.get("instrument")
        payload = event.get("payload")
        ts_local = event.get("ts_local")
        if not isinstance(instrument, InstrumentId) or not isinstance(payload, dict):
            return
        with self._lock:
            normalizer = self._normalizers.get(exchange)
        if normalizer is None:
            return
        try:
            quote = normalizer.normalize_l1(instrument=instrument, payload=payload, ts_local=int(ts_local or 0))
        except Exception as exc:
            self._logger.error("quote normalize failed | exchange=%s | symbol=%s | error=%s", exchange, instrument.symbol, exc)
            return
        self.publish_quote(quote)

    @staticmethod
    def _transport_key(instrument: InstrumentId) -> str:
        return f"{instrument.exchange}:{instrument.market_type}"

    def _start_depth20_worker_if_supported(self, instrument: InstrumentId) -> None:
        if instrument.exchange not in {"binance", "bitget"}:
            return
        with self._lock:
            if instrument in self._depth20_workers:
                return
            if instrument.exchange == "binance":
                worker = _BinanceDepth20Worker(instrument=instrument, on_snapshot=self._on_depth20_snapshot)
            else:
                worker = _BitgetDepthWorker(instrument=instrument, on_snapshot=self._on_depth20_snapshot)
            self._depth20_workers[instrument] = worker
        worker.start()

    def _stop_depth20_worker(self, instrument: InstrumentId) -> None:
        with self._lock:
            worker = self._depth20_workers.pop(instrument, None)
            self._depth20_cache.pop(instrument, None)
            self._depth20_seq.pop(instrument, None)
            self._depth20_diagnostics.pop(instrument, None)
        if worker is not None:
            worker.stop()

    def _on_depth20_snapshot(self, snapshot: QuoteDepth20) -> None:
        symbol_error = self._validate_depth_symbol(snapshot)
        if symbol_error is not None:
            self._mark_depth_reject(snapshot.instrument_id, symbol_error)
            return
        structure_error = self._validate_depth_structure(snapshot)
        if structure_error is not None:
            self._mark_depth_reject(snapshot.instrument_id, structure_error)
            return
        with self._lock:
            current_seq = int(self._depth20_seq.get(snapshot.instrument_id, 0))
            next_seq = current_seq + 1
            # Atomic snapshot write: runtime sees either old or fully new snapshot.
            atomic_snapshot = replace(snapshot, snapshot_id=next_seq)
            self._depth20_cache[snapshot.instrument_id] = atomic_snapshot
            self._depth20_seq[snapshot.instrument_id] = next_seq
            diag = self._depth20_diagnostics.setdefault(
                snapshot.instrument_id,
                {
                    "depth_updates_count": 0,
                    "depth_reject_count": 0,
                    "depth_last_reason": "NO_DATA",
                    "last_depth_ts_ms": None,
                },
            )
            diag["depth_updates_count"] = int(diag.get("depth_updates_count") or 0) + 1
            diag["depth_last_reason"] = "OK"
            diag["last_depth_ts_ms"] = int(atomic_snapshot.ts_local)

    @staticmethod
    def _normalize_ws_symbol(value: str | None) -> str:
        normalized = str(value or "").strip().upper()
        for separator in ("-", "_", "/"):
            normalized = normalized.replace(separator, "")
        return normalized

    def _validate_depth_symbol(self, snapshot: QuoteDepth20) -> str | None:
        instrument = snapshot.instrument_id
        expected = self._normalize_ws_symbol(instrument.routing.ws_symbol or instrument.symbol)
        actual = self._normalize_ws_symbol(snapshot.source_symbol)
        if not actual:
            return "DEPTH_REJECT_MISSING_SYMBOL"
        if expected and expected != actual:
            return f"DEPTH_REJECT_SYMBOL_MISMATCH expected={expected} actual={actual}"
        return None

    @staticmethod
    def _validate_depth_structure(snapshot: QuoteDepth20) -> str | None:
        if not snapshot.bids:
            return "DEPTH_REJECT_EMPTY_BIDS"
        if not snapshot.asks:
            return "DEPTH_REJECT_EMPTY_ASKS"
        previous_bid = None
        for level in snapshot.bids:
            if level.price <= Decimal("0") or level.quantity <= Decimal("0"):
                return "DEPTH_REJECT_INVALID_BID_LEVEL"
            if previous_bid is not None and level.price > previous_bid:
                return "DEPTH_REJECT_BIDS_NOT_SORTED"
            previous_bid = level.price
        previous_ask = None
        for level in snapshot.asks:
            if level.price <= Decimal("0") or level.quantity <= Decimal("0"):
                return "DEPTH_REJECT_INVALID_ASK_LEVEL"
            if previous_ask is not None and level.price < previous_ask:
                return "DEPTH_REJECT_ASKS_NOT_SORTED"
            previous_ask = level.price
        return None

    def _mark_depth_reject(self, instrument: InstrumentId, reason: str) -> None:
        with self._lock:
            diag = self._depth20_diagnostics.setdefault(
                instrument,
                {
                    "depth_updates_count": 0,
                    "depth_reject_count": 0,
                    "depth_last_reason": "NO_DATA",
                    "last_depth_ts_ms": None,
                },
            )
            diag["depth_reject_count"] = int(diag.get("depth_reject_count") or 0) + 1
            diag["depth_last_reason"] = str(reason or "DEPTH_REJECT_UNKNOWN")
