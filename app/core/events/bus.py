from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from typing import Any


class EventBus:
    """Simple in-process pub/sub for system and worker events."""

    def __init__(self) -> None:
        self._subscribers: dict[str, list[Callable[[Any], None]]] = defaultdict(list)

    def subscribe(self, topic: str, callback: Callable[[Any], None]) -> None:
        self._subscribers[topic].append(callback)

    def publish(self, topic: str, event: Any) -> None:
        for callback in list(self._subscribers.get(topic, [])):
            callback(event)
