from __future__ import annotations

import threading
import time
from collections.abc import Callable

from app.charts.market_data import load_funding_updates, load_price_spread_updates


class ChartLiveRefreshWorker:
    def __init__(
        self,
        *,
        on_price: Callable[[str, object, str], None],
        on_funding: Callable[[str, str, object, str], None],
    ) -> None:
        self._on_price = on_price
        self._on_funding = on_funding
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._wakeup = threading.Event()
        self._config_version = 0
        self._config: dict[str, object] = {}
        self._thread = threading.Thread(target=self._background_loop, name="chart-live-refresh", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wakeup.set()

    def update_config(self, config: dict[str, object]) -> None:
        with self._lock:
            self._config_version += 1
            self._config = dict(config)
        self._wakeup.set()

    def _background_loop(self) -> None:
        last_version = -1
        next_price_ts = 0.0
        next_funding_ts = 0.0
        while not self._stop.is_set():
            with self._lock:
                version = self._config_version
                config = dict(self._config)
            if version != last_version:
                last_version = version
                next_price_ts = 0.0
                next_funding_ts = 0.0
            symbols = [str(item).strip().upper() for item in list(config.get("symbols") or []) if str(item).strip()]
            if not symbols:
                self._wakeup.wait(0.5)
                self._wakeup.clear()
                continue
            monotonic_now = time.monotonic()
            did_work = False
            if monotonic_now >= next_price_ts:
                self._load_price_updates(config, symbols)
                next_price_ts = monotonic_now + 1.0
                did_work = True
            if monotonic_now >= next_funding_ts:
                self._load_funding_updates(config, symbols)
                next_funding_ts = monotonic_now + 10.0
                did_work = True
            timeout = 0.15 if did_work else 0.5
            self._wakeup.wait(timeout)
            self._wakeup.clear()

    def _load_price_updates(self, config: dict[str, object], symbols: list[str]) -> None:
        try:
            updates = load_price_spread_updates(
                left_exchange=str(config.get("left_exchange") or ""),
                left_market_type=str(config.get("left_market_type") or ""),
                right_exchange=str(config.get("right_exchange") or ""),
                right_market_type=str(config.get("right_market_type") or ""),
                symbols=symbols,
            )
            self._on_price(str(config.get("cache_key") or ""), updates, "")
        except Exception as exc:
            self._on_price(str(config.get("cache_key") or ""), None, str(exc))

    def _load_funding_updates(self, config: dict[str, object], symbols: list[str]) -> None:
        for side in ("left", "right"):
            try:
                updates = load_funding_updates(
                    exchange=str(config.get(f"{side}_exchange") or ""),
                    market_type=str(config.get(f"{side}_market_type") or ""),
                    symbols=symbols,
                )
                self._on_funding(str(config.get("cache_key") or ""), side, updates, "")
            except Exception as exc:
                self._on_funding(str(config.get("cache_key") or ""), side, None, str(exc))
