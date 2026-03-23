from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class EndpointSpec:
    key: str
    path: str


_ENDPOINTS: dict[str, dict[str, EndpointSpec]] = {
    "binance": {
        "usdm_exchange_info": EndpointSpec(key="usdm_exchange_info", path="/fapi/v1/exchangeInfo"),
        "usdm_24hr_ticker": EndpointSpec(key="usdm_24hr_ticker", path="/fapi/v1/ticker/24hr"),
        "usdm_premium_index": EndpointSpec(key="usdm_premium_index", path="/fapi/v1/premiumIndex"),
        "usdm_funding_info": EndpointSpec(key="usdm_funding_info", path="/fapi/v1/fundingInfo"),
    },
    "bybit": {
        "market_tickers": EndpointSpec(key="market_tickers", path="/v5/market/tickers"),
        "market_instruments_info": EndpointSpec(key="market_instruments_info", path="/v5/market/instruments-info"),
    },
}


def get_endpoint_spec(exchange_id: str, endpoint_key: str) -> EndpointSpec | None:
    exchange_map = _ENDPOINTS.get(str(exchange_id or "").strip().lower())
    if exchange_map is None:
        return None
    return exchange_map.get(str(endpoint_key or "").strip().lower())


__all__ = ["EndpointSpec", "get_endpoint_spec"]
