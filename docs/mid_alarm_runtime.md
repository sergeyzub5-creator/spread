# Mid-alarm runtime (spread entry)

Defer **depth20** and **execution adapters** until the spread by **mid** (L1 mids) crosses the entry threshold by magnitude. While `|mid_spread| >= entry_threshold`, a window (default **60s**) is armed and **extended on each touch**. Entry still requires **book edge** ≥ threshold (`calculate_spread_edges`); mid only wakes subscriptions.

## Params (`runtime_params`)

| Key | Default | Meaning |
|-----|---------|--------|
| `mid_alarm_enabled` | `1` from UI start | `1`/`true`/`on` — L1-only subscribe first; arm depth+privates on touch. |
| `mid_alarm_window_sec` | `60` | Armed window length; refreshed while mid keeps touching threshold. |

Disable mid-alarm (legacy behavior): set `mid_alarm_enabled` to `0` before start — depth20 and adapters connect immediately on worker start.

## MarketDataService

- `subscribe_l1(..., enable_depth20=False)` — public L1 only (no depth20 worker for binance/bitget).
- `ensure_depth20(instrument)` / `release_depth20(instrument)` — refcounted; last release stops the worker.

## Metrics

- `mid_alarm_active` — bool
- `mid_alarm_armed_until_ms` — epoch ms
- `mid_alarm_mid_mag` — formatted mid magnitude when armed

## Block reason

Entry blocked with `MID_ALARM_DISARMED` until first arm; throttled in logs like other noisy reasons.
