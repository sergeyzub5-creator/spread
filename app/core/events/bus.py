from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
import threading
from typing import Any

from app.core.logging.logger_factory import get_logger


class EventBus:
    """Simple in-process pub/sub for system and worker events."""

    def __init__(self) -> None:
        self._logger = get_logger("events.bus")
        self._lock = threading.RLock()
        self._subscribers: dict[str, list[Callable[[Any], None]]] = defaultdict(list)

    def subscribe(self, topic: str, callback: Callable[[Any], None]) -> None:
        with self._lock:
            self._subscribers[topic].append(callback)

    def unsubscribe(self, topic: str, callback: Callable[[Any], None]) -> None:
        with self._lock:
            callbacks = self._subscribers.get(topic)
            if not callbacks:
                return
            try:
                callbacks.remove(callback)
            except ValueError:
                return
            if not callbacks:
                self._subscribers.pop(topic, None)

    def publish(self, topic: str, event: Any) -> None:
        with self._lock:
            callbacks = list(self._subscribers.get(topic, []))
        for callback in callbacks:
            try:
                callback(event)
            except Exception:
                self._logger.exception("event subscriber failed | topic=%s | callback=%r", topic, callback)
