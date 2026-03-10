# Binance Connector Snapshot

Snapshot date: 2026-03-10

This file captures the current Binance connector setup in one place for review during runtime/policy refactor.

## Files In Use

- `app/core/market_data/binance_usdm_connector.py`
- `app/core/market_data/binance_spot_connector.py`

## Binance USD-M Public Connector

Class: `BinanceUsdmPublicConnector`

### Endpoint

- `wss://fstream.binance.com/ws`

### Current Stability Controls

- Hard reconnect path in `connect()` if socket thread is alive but disconnected.
- Disconnect grace before hard restart: `2000ms`.
- Send failure recovery in `_send(...)`: marks disconnected and closes ws.
- Stale stream watchdog:
  - `STALE_STREAM_TIMEOUT_MS = 10000`
  - `WATCHDOG_INTERVAL_SECONDS = 5.0`
  - if no messages for timeout window and subscriptions exist -> close socket and force restart.

### Stream Lifecycle

- `_on_open()`:
  - marks connected
  - resets disconnect timestamp
  - sets `_last_message_ts_ms`
  - starts watchdog loop
  - resubscribes active streams
- `_on_message()`:
  - parses payload
  - routes by `<symbol>@bookTicker`
  - updates `_last_message_ts_ms`
  - emits quote callbacks
- `_on_close()`:
  - marks disconnected
  - stores disconnect timestamp

## Binance Spot Public Connector

Class: `BinanceSpotPublicConnector`

### Endpoint

- `wss://stream.binance.com:9443/ws`

### Current Stability Controls

- Hard reconnect path in `connect()` for dead half-open sessions.
- Disconnect grace before hard restart: `2000ms`.
- Send failure recovery in `_send(...)`: marks disconnected and closes ws.
- Stale stream watchdog:
  - `STALE_STREAM_TIMEOUT_MS = 10000`
  - `WATCHDOG_INTERVAL_SECONDS = 5.0`
  - if no messages for timeout window and subscriptions exist -> close socket and force restart.

### Stream Lifecycle

- `_on_open()`:
  - marks connected
  - resets disconnect timestamp
  - sets `_last_message_ts_ms`
  - starts watchdog loop
  - resubscribes active streams
- `_on_message()`:
  - parses payload
  - routes by `<symbol>@bookTicker`
  - updates `_last_message_ts_ms`
  - emits quote callbacks
- `_on_close()`:
  - marks disconnected
  - stores disconnect timestamp

## Notes For Upcoming Rewrite

- Runtime policy refactor is now active (`new` policy branch), so connector behavior can be evolved independently from old entry/exit strategy logic.
- This snapshot is reference-only and does not change runtime import paths.
