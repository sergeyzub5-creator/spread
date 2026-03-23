# Сканер — отдельный пакет

Запуск:

```bash
python scanner.py
```

## Папка `app/ui/scanner/`

Всё, что относится к вкладке сканера, собрано в одном месте.

| Путь | Назначение |
|------|------------|
| `app/ui/scanner/tab.py` | Вкладка (фильтр объёма + Применить) |
| `app/ui/scanner/window.py` | Отдельное окно со сканером |
| `app/ui/scanner/settings_store.py` | Сохранение порога в `app/data/scanner_settings.json` |
| `app/ui/scanner/volume_parse.py` | Парсинг/формат K M B |
| `app/ui/scanner/__init__.py` | Реэкспорт `ScannerTab`, `ScannerWindow`, … |

## Биржи — `app/ui/scanner/exchanges/<биржа>/`

Для каждой биржи своя папка (пока заготовки):

- `exchanges/binance/`
- `exchanges/bybit/`
- `exchanges/bitget/`
- `exchanges/okx/`

Сюда потом добавляются клиенты, нормализация символов, объёмы 24h и т.д.

## Интеграция в AppWindow

```python
from app.ui.scanner import ScannerTab
self.scanner_tab = ScannerTab()
```

Старые пути `app.ui.tabs.scanner_tab` и `app.ui.scanner_window` удалены.
