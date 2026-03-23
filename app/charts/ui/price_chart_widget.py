from __future__ import annotations

from decimal import Decimal
from datetime import datetime
import time

from PySide6.QtCore import QPoint, QPointF, QRectF, Qt, Signal
from PySide6.QtGui import QColor, QFont, QPainter, QPen, QPolygonF
from PySide6.QtWidgets import QWidget

from app.charts.models import PriceCandle, PricePoint


class PriceChartWidget(QWidget):
    olderHistoryRequested = Signal(object)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._points: list[PricePoint] = []
        self._candles: list[PriceCandle] = []
        self._status_text = "Выберите инструмент"
        self._series_name = ""
        self._axis_width = 76
        self._left_plot_padding = 26
        self._right_plot_padding = 0
        self._data_right_padding = 110
        self._time_axis_height = 26
        self._default_visible_points = 180
        self._visible_points = self._default_visible_points
        self._right_edge_index: float | None = None
        self._auto_vertical_range = True
        self._manual_lower_bound: Decimal | None = None
        self._manual_upper_bound: Decimal | None = None
        self._pan_anchor_pos: QPoint | None = None
        self._pan_anchor_right_edge: float | None = None
        self._pan_anchor_vertical_bounds: tuple[Decimal, Decimal] | None = None
        self._axis_drag_anchor_pos: QPoint | None = None
        self._axis_drag_anchor_bounds: tuple[Decimal, Decimal] | None = None
        self._crosshair_pos: QPoint | None = None
        self._older_history_request_before_ms: int | None = None
        self._line_cache_key: tuple | None = None
        self._line_cache_polyline = QPolygonF()
        self._grid_cache_key: tuple | None = None
        self._grid_cache_y_ticks: list[Decimal] = []
        self._grid_cache_time_positions: list[tuple[int, float, str]] = []
        self._last_repaint_monotonic = 0.0
        self.setObjectName("priceChartWidget")
        self.setMinimumSize(720, 420)
        self.setMouseTracking(True)

    def set_points(self, points: list[PricePoint]) -> None:
        self._points = list(points)
        self._candles = []
        self._status_text = ""
        self._older_history_request_before_ms = None
        self._invalidate_render_caches()
        self._reset_view()
        self.update()

    def prepend_points(self, points: list[PricePoint]) -> None:
        incoming = list(points or [])
        if not incoming:
            return
        merged = {int(point.timestamp_ms): point for point in self._points}
        added_count = 0
        for point in incoming:
            timestamp_ms = int(point.timestamp_ms)
            if timestamp_ms in merged:
                continue
            merged[timestamp_ms] = point
            added_count += 1
        if added_count <= 0:
            return
        self._points = [merged[key] for key in sorted(merged)]
        if self._right_edge_index is not None:
            self._right_edge_index += added_count
        self._older_history_request_before_ms = None
        self._invalidate_render_caches()
        self.update()

    def set_candles(self, candles: list[PriceCandle]) -> None:
        self._candles = list(candles)
        self._points = []
        self._status_text = ""
        self._invalidate_render_caches()
        self._reset_view()
        self.update()

    def set_status_text(self, text: str) -> None:
        self._status_text = str(text or "").strip()
        self.update()

    def set_series_name(self, name: str) -> None:
        self._series_name = str(name or "").strip().upper()
        self.update()

    def clear(self) -> None:
        self._points = []
        self._candles = []
        self._older_history_request_before_ms = None
        self._status_text = "Выберите инструмент"
        self._series_name = ""
        self._invalidate_render_caches()
        self._reset_view()
        self.update()

    def latest_price(self) -> Decimal | None:
        if self._points:
            return self._points[-1].price
        if self._candles:
            return self._candles[-1].close_price
        return None

    def mousePressEvent(self, event) -> None:
        if event.button() != Qt.MouseButton.LeftButton:
            super().mousePressEvent(event)
            return
        if self._axis_rect().contains(event.position()):
            self._axis_drag_anchor_pos = event.pos()
            self._axis_drag_anchor_bounds = self._current_vertical_bounds_tuple()
            self._crosshair_pos = None
            self.setCursor(Qt.CursorShape.SizeVerCursor)
        elif self._content_rect().contains(event.position()):
            self._crosshair_pos = event.pos()
            self._pan_anchor_pos = event.pos()
            self._pan_anchor_right_edge = self._effective_right_edge_index()
            self._pan_anchor_vertical_bounds = self._current_vertical_bounds_tuple()
            self.setCursor(Qt.CursorShape.BlankCursor)
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event) -> None:
        if self._axis_drag_anchor_pos is not None and self._axis_drag_anchor_bounds is not None:
            delta_y = event.pos().y() - self._axis_drag_anchor_pos.y()
            zoom_factor = Decimal(str(max(0.2, 1.0 + (delta_y / 200.0))))
            lower_bound, upper_bound = self._axis_drag_anchor_bounds
            center = (lower_bound + upper_bound) / Decimal("2")
            half_span = ((upper_bound - lower_bound) / Decimal("2")) * zoom_factor
            self._set_manual_vertical_bounds(center - half_span, center + half_span)
            self._schedule_repaint()
        elif self._pan_anchor_pos is not None and self._pan_anchor_right_edge is not None and self._points:
            self._crosshair_pos = event.pos()
            content_rect = self._content_rect()
            if content_rect.width() > 0:
                delta_x = event.pos().x() - self._pan_anchor_pos.x()
                points_shift = (delta_x / max(1.0, content_rect.width())) * max(1, self._visible_points - 1)
                self._set_right_edge_index(self._pan_anchor_right_edge - points_shift)
            if content_rect.height() > 0 and self._pan_anchor_vertical_bounds is not None:
                delta_y = event.pos().y() - self._pan_anchor_pos.y()
                lower_bound, upper_bound = self._pan_anchor_vertical_bounds
                span = upper_bound - lower_bound
                offset_shift = (Decimal(str(delta_y)) / Decimal(str(max(1.0, content_rect.height())))) * span
                self._set_manual_vertical_bounds(lower_bound + offset_shift, upper_bound + offset_shift)
            self._schedule_repaint()
        else:
            if self._plot_rect().contains(event.position()) or self._time_axis_rect().contains(event.position()):
                self._crosshair_pos = event.pos()
                self._schedule_repaint()
            if self._axis_rect().contains(event.position()):
                self._crosshair_pos = None
                self.setCursor(Qt.CursorShape.SizeVerCursor)
            elif self._content_rect().contains(event.position()):
                self.setCursor(Qt.CursorShape.BlankCursor)
            else:
                self._crosshair_pos = None
                self.unsetCursor()
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event) -> None:
        self._pan_anchor_pos = None
        self._pan_anchor_right_edge = None
        self._pan_anchor_vertical_bounds = None
        self._axis_drag_anchor_pos = None
        self._axis_drag_anchor_bounds = None
        if self._axis_rect().contains(event.position()):
            self.setCursor(Qt.CursorShape.SizeVerCursor)
        elif self._content_rect().contains(event.position()):
            self.setCursor(Qt.CursorShape.BlankCursor)
        else:
            self.unsetCursor()
        self._invalidate_render_caches()
        self.update()
        super().mouseReleaseEvent(event)

    def leaveEvent(self, event) -> None:
        self._crosshair_pos = None
        self.unsetCursor()
        self.update()
        super().leaveEvent(event)

    def mouseDoubleClickEvent(self, event) -> None:
        if event.button() == Qt.MouseButton.LeftButton and self._axis_rect().contains(event.position()):
            self._reset_vertical_view()
            self._invalidate_render_caches()
            self.update()
        super().mouseDoubleClickEvent(event)

    def wheelEvent(self, event) -> None:
        angle_delta = event.angleDelta().y()
        if angle_delta == 0:
            super().wheelEvent(event)
            return
        step = 1 if angle_delta > 0 else -1
        if self._axis_rect().contains(event.position()) or bool(event.modifiers() & Qt.KeyboardModifier.ControlModifier):
            factor = Decimal("0.9") if step > 0 else Decimal("1.1")
            lower_bound, upper_bound = self._current_vertical_bounds_tuple()
            center = (lower_bound + upper_bound) / Decimal("2")
            half_span = ((upper_bound - lower_bound) / Decimal("2")) * factor
            self._set_manual_vertical_bounds(center - half_span, center + half_span)
            self._invalidate_render_caches()
            self.update()
            event.accept()
            return
        if self._content_rect().contains(event.position()) and self._points:
            self._zoom_horizontal(step, float(event.position().x()))
            self._invalidate_render_caches()
            self.update()
            event.accept()
            return
        super().wheelEvent(event)

    def paintEvent(self, _event) -> None:
        painter = QPainter(self)
        try:
            painter.setRenderHint(QPainter.RenderHint.Antialiasing)

            rect = self.rect()
            painter.fillRect(rect, QColor("#0b0b0c"))

            plot_rect = self._plot_rect()
            axis_rect = self._axis_rect()
            time_axis_rect = self._time_axis_rect()
            visible_points = self._visible_points_slice()

            self._draw_grid(painter, plot_rect, visible_points)
            self._draw_zero_line(painter, plot_rect, axis_rect, visible_points)
            self._draw_line_series(painter, plot_rect, visible_points)
            self._draw_axis(painter, axis_rect, visible_points)
            self._draw_current_marker(painter, plot_rect, axis_rect, visible_points)
            self._draw_time_axis(painter, time_axis_rect, plot_rect, visible_points)
            self._draw_crosshair(painter, plot_rect, axis_rect, time_axis_rect, visible_points)
            self._draw_status_overlay(painter, plot_rect)
        finally:
            painter.end()

    def _draw_grid(self, painter: QPainter, rect: QRectF, points: list[PricePoint]) -> None:
        grid_pen = QPen(QColor("#15181d"))
        grid_pen.setWidth(1)
        painter.setPen(grid_pen)
        y_ticks, time_positions = self._cached_grid_values(rect, points)
        for tick_value in y_ticks:
            y = self._price_to_y(rect, tick_value, points)
            painter.drawLine(int(rect.left()), int(y), int(rect.right()), int(y))
        for index, x, _label in time_positions:
            painter.drawLine(int(x), rect.top(), int(x), rect.bottom())

    def _draw_line_series(self, painter: QPainter, rect: QRectF, points: list[PricePoint]) -> None:
        if not points:
            return

        lower_bound, upper_bound = self._vertical_bounds(points)
        data_rect = self._data_rect(rect)
        polyline_points = self._cached_polyline(data_rect, rect, points, lower_bound, upper_bound)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, (not self._is_interacting()) and len(polyline_points) <= 1800)

        line_pen = QPen(QColor("#2962ff"))
        line_pen.setWidth(1 if self._is_interacting() else 2)
        painter.setPen(line_pen)
        if len(polyline_points) >= 2:
            painter.drawPolyline(polyline_points)

        last_point = polyline_points[-1]
        painter.setBrush(QColor("#2962ff"))
        painter.setPen(Qt.PenStyle.NoPen)
        painter.drawEllipse(last_point, 3.5, 3.5)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)

    def _auto_vertical_bounds(self, points: list[PricePoint] | None = None) -> tuple[Decimal, Decimal]:
        series = points if points is not None else self._visible_points_slice()
        values = [point.price for point in series]
        if not values:
            return (Decimal("-5"), Decimal("5"))
        lower_bound = min(values)
        upper_bound = max(values)
        span = upper_bound - lower_bound
        if span <= 0:
            span = max(abs(upper_bound), Decimal("1")) * Decimal("0.2")
        padding = max(span * Decimal("0.15"), Decimal("0.10"))
        lower_bound -= padding
        upper_bound += padding
        if lower_bound > 0:
            lower_bound = Decimal("0")
        if upper_bound < 0:
            upper_bound = Decimal("0")
        if upper_bound <= lower_bound:
            upper_bound = lower_bound + Decimal("1")
        return (lower_bound, upper_bound)

    def _vertical_bounds(self, points: list[PricePoint] | None = None) -> tuple[Decimal, Decimal]:
        if self._auto_vertical_range or self._manual_lower_bound is None or self._manual_upper_bound is None:
            return self._auto_vertical_bounds(points)
        lower_bound = self._manual_lower_bound
        upper_bound = self._manual_upper_bound
        if lower_bound > 0:
            lower_bound = Decimal("0")
        if upper_bound < 0:
            upper_bound = Decimal("0")
        if upper_bound <= lower_bound:
            upper_bound = lower_bound + Decimal("1")
        return (lower_bound, upper_bound)

    def _current_vertical_bounds_tuple(self) -> tuple[Decimal, Decimal]:
        return self._vertical_bounds(self._visible_points_slice())

    def _set_manual_vertical_bounds(self, lower_bound: Decimal, upper_bound: Decimal) -> None:
        normalized_lower = Decimal(str(lower_bound))
        normalized_upper = Decimal(str(upper_bound))
        if normalized_upper <= normalized_lower:
            normalized_upper = normalized_lower + Decimal("1")
        if normalized_lower > 0:
            normalized_lower = Decimal("0")
        if normalized_upper < 0:
            normalized_upper = Decimal("0")
        self._auto_vertical_range = False
        self._manual_lower_bound = normalized_lower
        self._manual_upper_bound = normalized_upper
        self._invalidate_render_caches()

    def _price_to_y(self, rect: QRectF, price: Decimal, points: list[PricePoint] | None = None) -> float:
        lower_bound, upper_bound = self._vertical_bounds(points)
        bounded_price = max(lower_bound, min(upper_bound, price))
        y_ratio = float((bounded_price - lower_bound) / (upper_bound - lower_bound))
        return rect.bottom() - (rect.height() * y_ratio)

    def _draw_zero_line(self, painter: QPainter, rect: QRectF, axis_rect: QRectF, points: list[PricePoint]) -> None:
        lower_bound, upper_bound = self._vertical_bounds(points)
        if not (lower_bound <= 0 <= upper_bound):
            return
        zero_y = self._price_to_y(rect, Decimal("0"), points)
        zero_pen = QPen(QColor("#8d98b2"))
        zero_pen.setWidth(2)
        painter.setPen(zero_pen)
        painter.drawLine(int(rect.left()), int(zero_y), int(axis_rect.right()), int(zero_y))

    def _format_percent_label(self, value: Decimal) -> str:
        return f"{value:+.2f}%".replace(".", ",")

    def _y_axis_ticks(self, points: list[PricePoint]) -> list[Decimal]:
        lower_bound, upper_bound = self._vertical_bounds(points)
        span = upper_bound - lower_bound
        if span <= 0:
            return [Decimal("0")]
        desired_step = span / Decimal("8")
        nice_steps = [
            Decimal("0.01"),
            Decimal("0.02"),
            Decimal("0.05"),
            Decimal("0.10"),
            Decimal("0.20"),
            Decimal("0.25"),
            Decimal("0.50"),
            Decimal("1.00"),
            Decimal("2.00"),
            Decimal("2.50"),
            Decimal("5.00"),
            Decimal("10.00"),
        ]
        step = nice_steps[-1]
        for candidate in nice_steps:
            if candidate >= desired_step:
                step = candidate
                break
        start_tick = (lower_bound // step) * step
        if start_tick > lower_bound:
            start_tick -= step
        ticks: list[Decimal] = []
        current = start_tick
        while current <= upper_bound + step:
            if lower_bound <= current <= upper_bound:
                ticks.append(current)
            current += step
            if len(ticks) > 200:
                break
        if Decimal("0") not in ticks and lower_bound <= 0 <= upper_bound:
            ticks.append(Decimal("0"))
            ticks.sort()
        return ticks

    def _draw_axis(self, painter: QPainter, rect: QRectF, points: list[PricePoint]) -> None:
        painter.fillRect(rect, QColor("#111214"))

        border_pen = QPen(QColor("#262a31"))
        border_pen.setWidth(1)
        painter.setPen(border_pen)
        painter.drawLine(int(rect.left()), int(rect.top()), int(rect.left()), int(rect.bottom()))

        font = QFont("Segoe UI")
        font.setPointSize(9)
        painter.setFont(font)
        y_ticks, _time_positions = self._cached_grid_values(self._plot_rect(), points)
        for tick_value in y_ticks:
            label = self._format_percent_label(tick_value)
            y = self._price_to_y(self._plot_rect(), tick_value, points)
            if label in ("+0,00%", "0,00%", "-0,00%"):
                painter.setPen(QColor("#f1f3f7"))
                label = "0,00%"
            else:
                painter.setPen(QColor("#c8cdd7"))
            painter.drawText(
                QRectF(rect.left() + 10, y - 10, rect.width() - 12, 20),
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                label,
            )
        self._draw_zero_axis_label(painter, rect, points)

    def _draw_zero_axis_label(self, painter: QPainter, rect: QRectF, points: list[PricePoint]) -> None:
        lower_bound, upper_bound = self._vertical_bounds(points)
        if not (lower_bound <= 0 <= upper_bound):
            return
        zero_y = self._price_to_y(self._plot_rect(), Decimal("0"), points)
        label_rect = QRectF(rect.left() + 6, zero_y - 11, rect.width() - 10, 22)
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(QColor("#1b2230"))
        painter.drawRoundedRect(label_rect, 6, 6)
        painter.setPen(QColor("#f1f3f7"))
        painter.drawText(label_rect.adjusted(8, 0, -4, 0), Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, "0,00%")

    def _draw_current_marker(self, painter: QPainter, plot_rect: QRectF, axis_rect: QRectF, points: list[PricePoint]) -> None:
        if not points:
            return
        latest_point = points[-1]
        latest_y = self._price_to_y(plot_rect, latest_point.price, points)
        dotted_pen = QPen(QColor("#d4d9e3"))
        dotted_pen.setWidth(1)
        dotted_pen.setStyle(Qt.PenStyle.DotLine)
        painter.setPen(dotted_pen)
        painter.drawLine(int(plot_rect.left()), int(latest_y), int(axis_rect.left()), int(latest_y))

        if self._series_name:
            symbol_rect = QRectF(axis_rect.left() - 92, latest_y - 12, 88, 24)
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(QColor("#d9dde6"))
            painter.drawRoundedRect(symbol_rect, 4, 4)
            painter.setPen(QColor("#111214"))
            symbol_font = QFont("Segoe UI")
            symbol_font.setPointSize(9)
            painter.setFont(symbol_font)
            painter.drawText(symbol_rect.adjusted(8, 0, -6, 0), Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, self._series_name)

        value_rect = QRectF(axis_rect.left() + 1, latest_y - 18, axis_rect.width() - 2, 36)
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(QColor("#d9dde6"))
        painter.drawRoundedRect(value_rect, 4, 4)
        painter.setPen(QColor("#111214"))
        value_font = QFont("Segoe UI")
        value_font.setPointSize(9)
        painter.setFont(value_font)
        latest_value = self._format_percent_label(latest_point.price).replace("+", "")
        latest_time = self._format_time_label(int(latest_point.timestamp_ms), 1)
        painter.drawText(value_rect.adjusted(8, 2, -6, -16), Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, latest_value)
        small_font = QFont("Segoe UI")
        small_font.setPointSize(8)
        painter.setFont(small_font)
        painter.drawText(value_rect.adjusted(8, 16, -6, -2), Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, latest_time)

    def _draw_time_axis(self, painter: QPainter, rect: QRectF, plot_rect: QRectF, points: list[PricePoint]) -> None:
        painter.fillRect(rect, QColor("#0b0b0c"))
        border_pen = QPen(QColor("#17191d"))
        border_pen.setWidth(1)
        painter.setPen(border_pen)
        painter.drawLine(int(plot_rect.left()), int(rect.top()), int(plot_rect.right()), int(rect.top()))
        font = QFont("Segoe UI")
        font.setPointSize(8)
        painter.setFont(font)
        painter.setPen(QColor("#aeb5c2"))
        _y_ticks, time_positions = self._cached_grid_values(plot_rect, points)
        for index, x, label in time_positions:
            if not label:
                continue
            label_rect = QRectF(x - 26, rect.top() + 4, 52, rect.height() - 6)
            alignment = Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignTop
            if index == 0:
                label_rect = QRectF(plot_rect.left(), rect.top() + 4, 52, rect.height() - 6)
                alignment = Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop
            painter.drawText(label_rect, alignment, label)

    def _draw_crosshair(
        self,
        painter: QPainter,
        plot_rect: QRectF,
        axis_rect: QRectF,
        time_axis_rect: QRectF,
        points: list[PricePoint],
    ) -> None:
        if not points or self._crosshair_pos is None:
            return
        crosshair = QPointF(self._crosshair_pos)
        if not (plot_rect.adjusted(-8, -4, 8, 4).contains(crosshair) or axis_rect.contains(crosshair) or time_axis_rect.contains(crosshair)):
            return
        clamped_x = max(plot_rect.left(), min(plot_rect.right(), crosshair.x()))
        clamped_y = max(plot_rect.top(), min(plot_rect.bottom(), crosshair.y()))
        dash_pen = QPen(QColor("#cfd5df"))
        dash_pen.setWidthF(0.8)
        dash_pen.setStyle(Qt.PenStyle.DashLine)
        painter.setPen(dash_pen)
        painter.drawLine(int(clamped_x), int(plot_rect.top()), int(clamped_x), int(plot_rect.bottom()))
        painter.drawLine(int(plot_rect.left()), int(clamped_y), int(axis_rect.right()), int(clamped_y))
        self._draw_crosshair_time_label(painter, time_axis_rect, plot_rect, points, clamped_x)
        self._draw_crosshair_value_label(painter, axis_rect, plot_rect, points, clamped_y)

    def _draw_crosshair_time_label(
        self,
        painter: QPainter,
        time_axis_rect: QRectF,
        plot_rect: QRectF,
        points: list[PricePoint],
        x: float,
    ) -> None:
        if not points:
            return
        point = self._nearest_point_for_x(plot_rect, points, x)
        if point is None:
            return
        label = self._format_hover_time_label(int(point.timestamp_ms))
        label_width = 108
        label_rect = QRectF(x - (label_width / 2), time_axis_rect.top() + 2, label_width, time_axis_rect.height() - 4)
        if label_rect.left() < plot_rect.left():
            label_rect.moveLeft(plot_rect.left())
        if label_rect.right() > plot_rect.right():
            label_rect.moveRight(plot_rect.right())
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(QColor("#4b4f57"))
        painter.drawRoundedRect(label_rect, 3, 3)
        painter.setPen(QColor("#f2f4f8"))
        font = QFont("Segoe UI")
        font.setPointSize(8)
        painter.setFont(font)
        painter.drawText(label_rect, Qt.AlignmentFlag.AlignCenter, label)

    def _draw_crosshair_value_label(
        self,
        painter: QPainter,
        axis_rect: QRectF,
        plot_rect: QRectF,
        points: list[PricePoint],
        y: float,
    ) -> None:
        if not points:
            return
        lower_bound, upper_bound = self._vertical_bounds(points)
        if plot_rect.height() <= 0:
            return
        ratio = (plot_rect.bottom() - y) / plot_rect.height()
        value = lower_bound + ((upper_bound - lower_bound) * Decimal(str(ratio)))
        label_rect = QRectF(axis_rect.left() + 1, y - 11, axis_rect.width() - 2, 22)
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(QColor("#4b4f57"))
        painter.drawRoundedRect(label_rect, 3, 3)
        painter.setPen(QColor("#f2f4f8"))
        font = QFont("Segoe UI")
        font.setPointSize(8)
        painter.setFont(font)
        painter.drawText(label_rect, Qt.AlignmentFlag.AlignCenter, self._format_percent_label(value).replace("+", ""))

    def _draw_status_overlay(self, painter: QPainter, rect: QRectF) -> None:
        if self._points or self._candles or not self._status_text:
            return
        painter.setPen(QColor("#9aa3b2"))
        font = QFont("Segoe UI")
        font.setPointSize(12)
        painter.setFont(font)
        painter.drawText(rect, Qt.AlignmentFlag.AlignCenter, self._status_text)

    def _content_rect(self) -> QRectF:
        return self._plot_rect()

    def _data_rect(self, plot_rect: QRectF) -> QRectF:
        usable_width = max(40.0, plot_rect.width() - self._data_right_padding)
        return QRectF(plot_rect.left(), plot_rect.top(), usable_width, plot_rect.height())

    def _plot_rect(self) -> QRectF:
        rect = self.rect()
        return QRectF(
            rect.left() + self._left_plot_padding,
            rect.top(),
            rect.width() - self._left_plot_padding - self._axis_width - self._right_plot_padding,
            rect.height() - self._time_axis_height,
        )

    def _axis_rect(self) -> QRectF:
        rect = self.rect()
        return QRectF(rect.right() - self._axis_width, rect.top(), self._axis_width, rect.height() - self._time_axis_height)

    def _time_axis_rect(self) -> QRectF:
        plot_rect = self._plot_rect()
        return QRectF(plot_rect.left(), plot_rect.bottom(), plot_rect.width(), self._time_axis_height)

    def _reset_view(self) -> None:
        self._visible_points = self._default_visible_points
        self._right_edge_index = None
        self._reset_vertical_view()
        self._pan_anchor_pos = None
        self._pan_anchor_right_edge = None
        self._pan_anchor_vertical_bounds = None
        self._axis_drag_anchor_pos = None
        self._axis_drag_anchor_bounds = None

    def _reset_vertical_view(self) -> None:
        self._auto_vertical_range = True
        self._manual_lower_bound = None
        self._manual_upper_bound = None

    def _effective_right_edge_index(self) -> float:
        if not self._points:
            return 0.0
        max_index = float(max(0, len(self._points) - 1))
        if self._right_edge_index is None:
            return max_index
        return max(0.0, min(max_index, float(self._right_edge_index)))

    def _set_right_edge_index(self, value: float) -> None:
        if not self._points:
            self._right_edge_index = 0.0
            return
        max_index = float(max(0, len(self._points) - 1))
        min_right_edge = float(min(max_index, max(0, self._visible_points - 1)))
        new_value = max(min_right_edge, min(max_index, float(value)))
        if self._right_edge_index != new_value:
            self._right_edge_index = new_value
            self._invalidate_render_caches()

    def _visible_points_slice(self) -> list[PricePoint]:
        if not self._points:
            return []
        total_points = len(self._points)
        visible_count = max(2, min(total_points, int(self._visible_points)))
        right_edge = self._effective_right_edge_index()
        end_index = int(round(right_edge))
        end_index = max(visible_count - 1, min(total_points - 1, end_index))
        start_index = max(0, end_index - visible_count + 1)
        end_index = min(total_points - 1, start_index + visible_count - 1)
        self._maybe_request_older_history(start_index)
        return self._points[start_index : end_index + 1]

    def _maybe_request_older_history(self, start_index: int) -> None:
        if not self._points or start_index > 30:
            return
        oldest_loaded_ts = int(self._points[0].timestamp_ms)
        if self._older_history_request_before_ms == oldest_loaded_ts:
            return
        self._older_history_request_before_ms = oldest_loaded_ts
        self.olderHistoryRequested.emit(oldest_loaded_ts)

    def _time_grid_positions(self, rect: QRectF, points: list[PricePoint]) -> list[tuple[int, float, str]]:
        if not points:
            return []
        if len(points) == 1:
            timestamp_ms = int(points[0].timestamp_ms)
            return [(0, rect.left(), self._format_time_label(timestamp_ms, 1))]

        first_ts = int(points[0].timestamp_ms)
        last_ts = int(points[-1].timestamp_ms)
        if last_ts <= first_ts:
            return [(0, rect.left(), self._format_time_label(first_ts, 1))]

        interval_minutes = self._choose_time_grid_interval_minutes(points)
        interval_ms = interval_minutes * 60_000
        first_tick_ts = (first_ts // interval_ms) * interval_ms

        positions: list[tuple[int, float, str]] = []
        tick_ts = first_tick_ts
        while tick_ts <= last_ts:
            ratio = (tick_ts - first_ts) / max(1, last_ts - first_ts)
            x = rect.left() + (rect.width() * ratio)
            if rect.left() <= x <= rect.right():
                positions.append((len(positions), x, self._format_time_label(tick_ts, interval_minutes)))
            tick_ts += interval_ms
        return positions

    def _choose_time_grid_interval_minutes(self, points: list[PricePoint]) -> int:
        if len(points) < 2:
            return 1
        total_minutes = max(1, int(round((int(points[-1].timestamp_ms) - int(points[0].timestamp_ms)) / 60_000)))
        target_lines = 6
        desired_step = max(1, total_minutes / target_lines)
        nice_steps = [1, 2, 5, 10, 15, 30, 60, 120, 180, 240, 360, 720, 1440]
        for step in nice_steps:
            if step >= desired_step:
                return step
        return nice_steps[-1]

    def _format_time_label(self, timestamp_ms: int, interval_minutes: int) -> str:
        dt = datetime.fromtimestamp(timestamp_ms / 1000)
        if interval_minutes >= 1440:
            return dt.strftime("%d.%m")
        if interval_minutes >= 180:
            return dt.strftime("%d %H:%M")
        return dt.strftime("%H:%M")

    def _format_hover_time_label(self, timestamp_ms: int) -> str:
        dt = datetime.fromtimestamp(timestamp_ms / 1000)
        month_names = {
            1: "янв",
            2: "фев",
            3: "мар",
            4: "апр",
            5: "мая",
            6: "июн",
            7: "июл",
            8: "авг",
            9: "сен",
            10: "окт",
            11: "ноя",
            12: "дек",
        }
        weekday_names = {
            0: "пн",
            1: "вт",
            2: "ср",
            3: "чт",
            4: "пт",
            5: "сб",
            6: "вс",
        }
        return f"{weekday_names[dt.weekday()]} {dt.day} {month_names[dt.month]} '{dt.strftime('%y')}  {dt.strftime('%H:%M')}"

    def _nearest_point_for_x(self, rect: QRectF, points: list[PricePoint], x: float) -> PricePoint | None:
        if not points:
            return None
        if len(points) == 1:
            return points[0]
        ratio = max(0.0, min(1.0, (x - rect.left()) / max(1.0, rect.width())))
        index = int(round((len(points) - 1) * ratio))
        index = max(0, min(len(points) - 1, index))
        return points[index]

    def _zoom_horizontal(self, step: int, cursor_x: float) -> None:
        total_points = len(self._points)
        if total_points < 2:
            return
        old_visible = max(2, min(total_points, int(self._visible_points)))
        content_rect = self._content_rect()
        relative_x = 0.5
        if content_rect.width() > 0:
            relative_x = max(0.0, min(1.0, (cursor_x - content_rect.left()) / content_rect.width()))
        current_right_edge = self._effective_right_edge_index()
        current_left_edge = current_right_edge - old_visible + 1
        anchor_index = current_left_edge + (old_visible - 1) * relative_x
        zoom_factor = 0.85 if step > 0 else 1.18
        new_visible = max(20, min(total_points, int(round(old_visible * zoom_factor))))
        if new_visible == old_visible:
            return
        new_left_edge = anchor_index - (new_visible - 1) * relative_x
        new_right_edge = new_left_edge + new_visible - 1
        self._visible_points = new_visible
        self._set_right_edge_index(new_right_edge)

    def _cached_polyline(
        self,
        data_rect: QRectF,
        plot_rect: QRectF,
        points: list[PricePoint],
        lower_bound: Decimal,
        upper_bound: Decimal,
    ) -> QPolygonF:
        if not points:
            return QPolygonF()
        key = (
            len(points),
            int(points[0].timestamp_ms),
            int(points[-1].timestamp_ms),
            round(data_rect.left(), 2),
            round(data_rect.width(), 2),
            round(plot_rect.bottom(), 2),
            round(plot_rect.height(), 2),
            str(lower_bound),
            str(upper_bound),
        )
        if self._line_cache_key == key and not self._line_cache_polyline.isEmpty():
            return self._line_cache_polyline
        polyline_points: list[QPointF] = []
        for index, point in enumerate(points):
            x_ratio = index / max(1, len(points) - 1)
            bounded_price = max(lower_bound, min(upper_bound, point.price))
            y_ratio = float((bounded_price - lower_bound) / (upper_bound - lower_bound))
            x = data_rect.left() + (data_rect.width() * x_ratio)
            y = plot_rect.bottom() - (plot_rect.height() * y_ratio)
            polyline_points.append(QPointF(x, y))
        self._line_cache_key = key
        self._line_cache_polyline = QPolygonF(polyline_points)
        return self._line_cache_polyline

    def _cached_grid_values(self, rect: QRectF, points: list[PricePoint]) -> tuple[list[Decimal], list[tuple[int, float, str]]]:
        if not points:
            return ([], [])
        lower_bound, upper_bound = self._vertical_bounds(points)
        key = (
            round(rect.left(), 2),
            round(rect.width(), 2),
            round(rect.height(), 2),
            len(points),
            int(points[0].timestamp_ms),
            int(points[-1].timestamp_ms),
            str(lower_bound),
            str(upper_bound),
        )
        if self._grid_cache_key == key:
            return (self._grid_cache_y_ticks, self._grid_cache_time_positions)
        self._grid_cache_key = key
        self._grid_cache_y_ticks = self._y_axis_ticks(points)
        self._grid_cache_time_positions = self._time_grid_positions(rect, points)
        return (self._grid_cache_y_ticks, self._grid_cache_time_positions)

    def _invalidate_render_caches(self) -> None:
        self._line_cache_key = None
        self._line_cache_polyline = QPolygonF()
        self._grid_cache_key = None
        self._grid_cache_y_ticks = []
        self._grid_cache_time_positions = []

    def _is_interacting(self) -> bool:
        return self._pan_anchor_pos is not None or self._axis_drag_anchor_pos is not None

    def _schedule_repaint(self, *, min_interval_ms: float = 16.0) -> None:
        now = time.monotonic() * 1000.0
        if now - self._last_repaint_monotonic >= min_interval_ms:
            self._last_repaint_monotonic = now
            self.update()
