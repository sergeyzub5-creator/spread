from __future__ import annotations

from PySide6.QtCore import QObject, Signal


TRANSLATIONS: dict[str, dict[str, str]] = {
    "ru": {
        "language.ru": "Русский",
        "language.en": "Английский",
        "app.window_title": "Spread Sniper",
        "top.settings": "Настройки",
        "top.language_tooltip": "Язык",
        "top.themes": "Темы",
        "theme.dark": "Темная",
        "theme.steel": "Сталь",
        "theme.graphite_pro": "Графит Pro",
        "tab.exchanges": "Биржи",
        "tab.spread": "Снайпинг спреда",
        "tab.test": "Тест",
        "status.active_tab": "Активная вкладка: {name}",
        "status.network.checking": "Проверка сети...",
        "status.network.online": "Интернет доступен",
        "status.network.offline": "Нет подключения к интернету",
        "status.warning": "Предупреждение: {message}",
        "placeholder.test_title": "Тест",
        "placeholder.test_subtitle": "Песочница для новых блоков. Здесь можно временно собирать макеты и экспериментальные элементы интерфейса.",
        "placeholder.action.show_block": "Показать блок",
        "placeholder.action.change_state": "Сменить состояние",
        "placeholder.action.reset": "Сбросить",
        "placeholder.block": "БЛОК {index}",
        "placeholder.block_title": "Временный интерактивный блок {index}",
        "placeholder.block_text": "Эта область оставлена облегченной и готова к полной переработке.",
        "placeholder.ready": "Готово",
        "placeholder.clicked": "Нажато: {label}",
        "exchanges.add": "Добавить биржу",
        "exchanges.connect_all": "Подключить все",
        "exchanges.disconnect_all": "Отключить все",
        "exchanges.close_all_positions": "Закрыть все позиции",
        "exchange_dialog.title": "Новое подключение · {exchange}",
        "exchange.new_connection": "Новое подключение",
        "exchange.picker_title": "Выбор биржи",
        "exchange.picker_subtitle": "Выберите биржу для нового подключения",
        "exchange.picker_add": "Добавить",
        "common.cancel": "Отмена",
        "exchange.status.connected": "Подключено",
        "exchange.status.disconnected": "Отключено",
        "exchange.balance": "Баланс: --",
        "exchange.positions": "Позиции: --",
        "exchange.pnl": "PnL: 0.00",
        "exchange.api_group": "API · {exchange}",
        "exchange.api_key": "API ключ",
        "exchange.api_secret": "API секрет",
        "exchange.passphrase": "Пассфраза",
        "exchange.passphrase_optional": "Пассфраза (необязательно)",
        "exchange.connect": "Подключить",
        "exchange.disconnect": "Отключить",
        "exchange.close_positions": "Закрыть позиции",
        "exchange.edit": "Изменить",
        "exchange.remove": "Удалить",
        "exchange.add_connection": "Добавить",
        "exchange.error.key_secret_required": "API ключ и секрет обязательны.",
        "exchange.error.key_secret_ascii": "API ключ и секрет должны быть в ASCII.",
        "exchange.error.passphrase_required": "Пассфраза обязательна.",
        "exchange.error.passphrase_ascii": "Пассфраза должна быть в ASCII.",
        "spread.choose_exchange": "Выбрать биржу",
        "spread.choose_type": "Выбрать тип",
        "spread.choose_instrument": "Выбрать инструмент",
        "spread.enter_symbol": "Введите символ",
        "spread.select": "ВЫБРАТЬ",
        "spread.strategy": "Стратегия",
        "spread.entry_threshold": "Порог входа",
        "spread.exit_threshold": "Порог выхода",
        "spread.target_size": "Целевой объем",
        "spread.step_size": "Шаг объема",
        "spread.max_slippage": "Макс. проскальзывание",
        "spread.bid": "Бид: --",
        "spread.ask": "Аск: --",
        "spread.qty": "Объем: --",
        "spread.bid_value": "Бид: {value}",
        "spread.ask_value": "Аск: {value}",
        "spread.qty_value": "Объем: {value} USDT",
        "splash.title": "Спред-снайпер",
        "splash.loading": "Загрузка интерфейса...",
        "splash.closing": "Завершение работы...",
        "exchange.unknown_title": "Неизвестная биржа",
        "exchange.unknown_base_name": "Биржа",
    },
    "en": {
        "language.ru": "Russian",
        "language.en": "English",
        "app.window_title": "Spread Sniper",
        "top.settings": "Settings",
        "top.language_tooltip": "Language",
        "top.themes": "Themes",
        "theme.dark": "Dark",
        "theme.steel": "Steel",
        "theme.graphite_pro": "Graphite Pro",
        "tab.exchanges": "Exchanges",
        "tab.spread": "Spread Sniping",
        "tab.test": "Test",
        "status.active_tab": "Active tab: {name}",
        "status.network.checking": "Checking network...",
        "status.network.online": "Internet connection available",
        "status.network.offline": "No internet connection",
        "status.warning": "Warning: {message}",
        "placeholder.test_title": "Test",
        "placeholder.test_subtitle": "Sandbox for new blocks. Temporary layouts and experimental interface elements can be assembled here.",
        "placeholder.action.show_block": "Show block",
        "placeholder.action.change_state": "Change state",
        "placeholder.action.reset": "Reset",
        "placeholder.block": "BLOCK {index}",
        "placeholder.block_title": "Interactive placeholder {index}",
        "placeholder.block_text": "This area is intentionally lightweight and ready for a full rewrite.",
        "placeholder.ready": "Ready",
        "placeholder.clicked": "Clicked: {label}",
        "exchanges.add": "Add exchange",
        "exchanges.connect_all": "Connect all",
        "exchanges.disconnect_all": "Disconnect all",
        "exchanges.close_all_positions": "Close all positions",
        "exchange_dialog.title": "New connection · {exchange}",
        "exchange.new_connection": "New connection",
        "exchange.picker_title": "Select exchange",
        "exchange.picker_subtitle": "Choose an exchange for the new connection",
        "exchange.picker_add": "Add",
        "common.cancel": "Cancel",
        "exchange.status.connected": "Connected",
        "exchange.status.disconnected": "Disconnected",
        "exchange.balance": "Balance: --",
        "exchange.positions": "Positions: --",
        "exchange.pnl": "PnL: 0.00",
        "exchange.api_group": "API · {exchange}",
        "exchange.api_key": "API Key",
        "exchange.api_secret": "API Secret",
        "exchange.passphrase": "Passphrase",
        "exchange.passphrase_optional": "Passphrase (optional)",
        "exchange.connect": "Connect",
        "exchange.disconnect": "Disconnect",
        "exchange.close_positions": "Close positions",
        "exchange.edit": "Edit",
        "exchange.remove": "Remove",
        "exchange.add_connection": "Add",
        "exchange.error.key_secret_required": "API key and secret are required.",
        "exchange.error.key_secret_ascii": "API key and secret must be ASCII.",
        "exchange.error.passphrase_required": "Passphrase is required.",
        "exchange.error.passphrase_ascii": "Passphrase must be ASCII.",
        "spread.choose_exchange": "Choose exchange",
        "spread.choose_type": "Choose type",
        "spread.choose_instrument": "Choose instrument",
        "spread.enter_symbol": "Enter symbol",
        "spread.select": "SELECT",
        "spread.strategy": "Strategy",
        "spread.entry_threshold": "Entry threshold",
        "spread.exit_threshold": "Exit threshold",
        "spread.target_size": "Target size",
        "spread.step_size": "Step size",
        "spread.max_slippage": "Max slippage",
        "spread.bid": "Bid: --",
        "spread.ask": "Ask: --",
        "spread.qty": "Qty: --",
        "spread.bid_value": "Bid: {value}",
        "spread.ask_value": "Ask: {value}",
        "spread.qty_value": "Qty: {value} USDT",
        "splash.title": "Spread Sniper",
        "splash.loading": "Loading interface...",
        "splash.closing": "Shutting down...",
        "exchange.unknown_title": "Unknown exchange",
        "exchange.unknown_base_name": "Exchange",
    },
}


class UiLanguageManager(QObject):
    language_changed = Signal(str)

    def __init__(self) -> None:
        super().__init__()
        self._language = "ru"

    def language(self) -> str:
        return self._language

    def set_language(self, language_code: str) -> None:
        normalized = str(language_code or "").strip().lower()
        if normalized not in TRANSLATIONS:
            normalized = "ru"
        if normalized == self._language:
            return
        self._language = normalized
        self.language_changed.emit(self._language)

    def available_languages(self) -> list[str]:
        return list(TRANSLATIONS)

    def translate(self, key: str, **kwargs) -> str:
        text = TRANSLATIONS.get(self._language, {}).get(key)
        if text is None:
            text = TRANSLATIONS["ru"].get(key, key)
        if kwargs:
            try:
                return text.format(**kwargs)
            except Exception:
                return text
        return text


_LANGUAGE_MANAGER = UiLanguageManager()


def get_language_manager() -> UiLanguageManager:
    return _LANGUAGE_MANAGER


def tr(key: str, **kwargs) -> str:
    return _LANGUAGE_MANAGER.translate(key, **kwargs)
