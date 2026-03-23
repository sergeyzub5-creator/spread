from __future__ import annotations

import json
from pathlib import Path


_STATE_PATH = Path(__file__).resolve().parents[3] / "data" / "chart_window_settings.json"


def load_chart_window_settings() -> dict:
    if not _STATE_PATH.exists():
        return {}
    try:
        with open(_STATE_PATH, encoding="utf-8") as file:
            data = json.load(file)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_payload(payload: dict) -> None:
    _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(_STATE_PATH, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def build_chart_selection_cache_key(
    left_exchange: str,
    left_market_type: str,
    right_exchange: str,
    right_market_type: str,
    volume_threshold: int | None,
) -> str:
    threshold = int(volume_threshold or 0)
    return "|".join(
        [
            str(left_exchange or "").strip().lower(),
            str(left_market_type or "").strip().lower(),
            str(right_exchange or "").strip().lower(),
            str(right_market_type or "").strip().lower(),
            str(threshold),
        ]
    )


def save_chart_window_settings(
    *,
    left_exchange: str | None = None,
    right_exchange: str | None = None,
    left_market_type: str | None = None,
    right_market_type: str | None = None,
    daily_volume_text: str | None = None,
    daily_volume_threshold: int | None = None,
    cached_pairs_by_selection: dict[str, list[str]] | None = None,
    bookmark_order_by_selection: dict[str, list[str]] | None = None,
    cached_rows_by_selection: dict[str, list[dict]] | None = None,
) -> None:
    payload = load_chart_window_settings()
    if left_exchange is not None:
        payload["left_exchange"] = str(left_exchange).strip().lower()
    if right_exchange is not None:
        payload["right_exchange"] = str(right_exchange).strip().lower()
    if left_market_type is not None:
        payload["left_market_type"] = str(left_market_type).strip().lower()
    if right_market_type is not None:
        payload["right_market_type"] = str(right_market_type).strip().lower()
    if daily_volume_text is not None:
        payload["daily_volume_text"] = str(daily_volume_text or "")
    if daily_volume_threshold is not None:
        payload["daily_volume_threshold"] = int(daily_volume_threshold)
    if cached_pairs_by_selection is not None:
        payload["cached_pairs_by_selection"] = {
            str(key): [str(item).strip().upper() for item in values if str(item).strip()]
            for key, values in cached_pairs_by_selection.items()
            if isinstance(values, list)
        }
        payload.pop("cached_rows_by_selection", None)
    if bookmark_order_by_selection is not None:
        payload["bookmark_order_by_selection"] = {
            str(key): [str(item).strip().upper() for item in values if str(item).strip()]
            for key, values in bookmark_order_by_selection.items()
            if isinstance(values, list)
        }
    if cached_rows_by_selection is not None:
        normalized_rows: dict[str, list[dict]] = {}
        for key, values in cached_rows_by_selection.items():
            if not isinstance(values, list):
                continue
            rows: list[dict] = []
            for item in values:
                if not isinstance(item, dict):
                    continue
                symbol = str(item.get("symbol") or "").strip().upper()
                if not symbol:
                    continue
                rows.append(
                    {
                        "symbol": symbol,
                        "spread_pct": item.get("spread_pct"),
                        "left_funding_rate": item.get("left_funding_rate"),
                        "left_funding_interval_hours": item.get("left_funding_interval_hours"),
                        "left_next_funding_ms": item.get("left_next_funding_ms"),
                        "right_funding_rate": item.get("right_funding_rate"),
                        "right_funding_interval_hours": item.get("right_funding_interval_hours"),
                        "right_next_funding_ms": item.get("right_next_funding_ms"),
                    }
                )
            normalized_rows[str(key)] = rows
        payload["cached_rows_by_selection"] = normalized_rows
    _write_payload(payload)
