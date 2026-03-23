from app.charts.ui.price_chart_window_data import PriceChartWindowDataMixin
from app.charts.ui.price_chart_window_state import PriceChartWindowStateMixin
from app.charts.ui.price_chart_window_table import PriceChartWindowTableMixin


class PriceChartWindowLogicMixin(
    PriceChartWindowStateMixin,
    PriceChartWindowTableMixin,
    PriceChartWindowDataMixin,
):
    pass
