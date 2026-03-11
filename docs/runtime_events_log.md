# Лог по умолчанию: только события

## Как сейчас

- **`logs/session_trace.log`** — **по умолчанию только события** (`emit_event`), одна строка = один JSON. Запуск = файл обнуляется.
- **Полный лог** (вся простыня INFO как раньше) — **опционально**:  
  `SPREAD_SNIPER_FULL_SESSION_LOG=1` → пишется в **`logs/session_trace_full.log`**.

Ничего выставлять не нужно: просто `py 1.py` — события уже идут в `session_trace.log`.

## Опционально

| Переменная | Эффект |
|------------|--------|
| `SPREAD_SNIPER_FULL_SESSION_LOG=1` | Дополнительно полный текстовый лог в `session_trace_full.log` |
| `SPREAD_SNIPER_EVENTS_LOG=0` | Не писать события в `session_trace.log` |
| `SPREAD_SNIPER_EVENTS_LOG_EXCLUDE=a,b` | Не писать события, где `event_type` содержит подстроку `a` или `b` |
| `SPREAD_SNIPER_EVENTS_LOG_ALL=1` | Писать **все** типы событий (в т.ч. тики котировок и дубликаты `*_order_event` / `rest_poll_*` и т.д.) — по умолчанию они **отфильтрованы** |
| `SPREAD_SNIPER_EVENTS_LOG_COMPACT=0` | Не ужимать payload: оставить `raw` и `null` (строки длиннее; для отчёта обычно не нужно) |

По умолчанию **не пишутся** (чтобы лог был пригоден для отчёта за часы): тики котировок; `left/right_order_event`, `entry_left/right_event` (сырой поток); `rest_poll_*`; `dual_exec_attempts_bound` / `entry_attempts_bound`; дубликаты `*_sent`, `entry_*_ack`, `entry_*_fill`; `execution_stream_health_updated` (тяжёлый payload). Остаются, например: `runtime_started` / `runtime_stopped`, `entry_signal_detected`, `entry_started`, `left_order_ack` / `right_order_ack`, `left_order_filled` / `right_order_filled`, `dual_exec_done`, `entry_done`, `execution_event_anomaly`, `order_failed` / `entry_failed`.

## Формат строки в session_trace.log

Первая строка после сброса — маркер схемы (`_schema`, `note`). Дальше одна строка = один JSON:

```json
{"worker_id":"...","event_type":"entry_done","timestamp":...,"payload":{...}}
```

В payload по умолчанию **нет** поля `raw` (ответ биржи) и пустых `null` — только факты для отчёта.
