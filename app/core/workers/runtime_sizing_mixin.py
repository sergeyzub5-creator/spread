from __future__ import annotations

import time
from decimal import Decimal, ROUND_DOWN
from math import gcd
from typing import TYPE_CHECKING, Any

from app.core.models.instrument import InstrumentId
from app.core.models.market_data import QuoteL1
from app.core.models.workers import EntryDecision

if TYPE_CHECKING:
    from app.core.workers.runtime_core import SpreadEdgeResult
    from app.core.workers.entry_validator import EntryValidationResult


class WorkerRuntimeSizingMixin:
    @staticmethod
    def _edge_name_from_actions(*, left_action: str | None, right_action: str | None) -> str | None:
        normalized_left = str(left_action or "").strip().upper()
        normalized_right = str(right_action or "").strip().upper()
        if normalized_left == "SELL" and normalized_right == "BUY":
            return "edge_1"
        if normalized_left == "BUY" and normalized_right == "SELL":
            return "edge_2"
        return None

    def _make_entry_decision(
        self,
        *,
        edge_result: SpreadEdgeResult,
        threshold: Decimal,
        validation_result: EntryValidationResult | None,
        block_reason: str | None,
        forced_signal: bool,
        is_executable: bool,
        planned_size: dict[str, Decimal] | None = None,
    ) -> EntryDecision:
        direction = (
            f"LEFT_{edge_result.left_action}_RIGHT_{edge_result.right_action}"
            if edge_result.left_action and edge_result.right_action
            else None
        )
        return EntryDecision(
            edge=edge_result.best_edge,
            direction=direction,
            threshold=threshold,
            validation_result=validation_result.to_dict() if validation_result is not None else {"is_valid": False, "block_reason": block_reason},
            planned_size=planned_size or self._planned_entry_size(edge_result),
            edge_name=self._edge_name_from_actions(left_action=edge_result.left_action, right_action=edge_result.right_action),
            left_action=str(edge_result.left_action or "") or None,
            right_action=str(edge_result.right_action or "") or None,
            block_reason=block_reason,
            is_executable=is_executable,
            forced_signal=forced_signal,
        )

    def _planned_entry_size(self, edge_result: SpreadEdgeResult) -> dict[str, Decimal]:
        left_qty, right_qty = self._current_test_sizes(edge_result)
        cycle_notional = self._effective_entry_cycle_notional_usdt(edge_result)
        left_qty, right_qty = self._apply_depth20_entry_volume_limit(
            edge_result=edge_result,
            left_qty=left_qty,
            right_qty=right_qty,
        )
        return {
            "entry_notional_usdt": self._entry_notional_usdt(),
            "cycle_notional_usdt": cycle_notional,
            "left_qty": left_qty,
            "right_qty": right_qty,
        }

    def _apply_depth20_entry_volume_limit(
        self,
        *,
        edge_result: SpreadEdgeResult,
        left_qty: Decimal,
        right_qty: Decimal,
    ) -> tuple[Decimal, Decimal]:
        if left_qty <= Decimal("0") or right_qty <= Decimal("0"):
            return left_qty, right_qty
        if str(edge_result.left_action or "").strip().upper() not in {"BUY", "SELL"}:
            return left_qty, right_qty
        if str(edge_result.right_action or "").strip().upper() not in {"BUY", "SELL"}:
            return left_qty, right_qty
        use_depth20 = str(self.task.runtime_params.get("entry_use_depth20_liquidity") or "1").strip().lower() not in {"0", "false", "off", "no"}
        if not use_depth20:
            return left_qty, right_qty
        left_snapshot = self.market_data_service.get_depth20_snapshot(self._left_instrument)
        right_snapshot = self.market_data_service.get_depth20_snapshot(self._right_instrument)
        if left_snapshot is None or right_snapshot is None:
            return left_qty, right_qty
        slippage_pct = self._entry_max_slippage_pct()
        left_available = self._depth20_available_qty_for_action(
            instrument=self._left_instrument,
            action=str(edge_result.left_action or ""),
            slippage_pct=slippage_pct,
        )
        right_available = self._depth20_available_qty_for_action(
            instrument=self._right_instrument,
            action=str(edge_result.right_action or ""),
            slippage_pct=slippage_pct,
        )
        buffer_pct = self._decimal_or_zero(self.task.runtime_params.get("entry_depth_buffer_pct"))
        if buffer_pct <= Decimal("0"):
            buffer_pct = Decimal("30")
        buffer_multiplier = Decimal("1") + (buffer_pct / Decimal("100"))
        if buffer_multiplier <= Decimal("0"):
            buffer_multiplier = Decimal("1")
        left_safe_available = left_available / buffer_multiplier
        right_safe_available = right_available / buffer_multiplier
        requested_common_qty = min(left_qty, right_qty)
        available_common_qty = min(left_safe_available, right_safe_available)
        limited_common_qty = min(requested_common_qty, available_common_qty)
        common_step = self._common_step_size(self._left_instrument.spec.qty_precision, self._right_instrument.spec.qty_precision)
        if common_step > Decimal("0"):
            limited_common_qty = (limited_common_qty / common_step).to_integral_value(rounding=ROUND_DOWN) * common_step
        min_common_qty = max(self._left_instrument.spec.min_qty, self._right_instrument.spec.min_qty)
        if limited_common_qty < min_common_qty:
            limited_common_qty = Decimal("0")
        if limited_common_qty >= requested_common_qty:
            return left_qty, right_qty
        self.logger.info(
            "entry depth20 liquidity clamp | requested_qty=%s | left_available_qty=%s | right_available_qty=%s | left_safe_qty=%s | right_safe_qty=%s | limited_qty=%s | slippage_pct=%s | depth_buffer_pct=%s",
            self._format_order_size(requested_common_qty),
            self._format_order_size(left_available),
            self._format_order_size(right_available),
            self._format_order_size(left_safe_available),
            self._format_order_size(right_safe_available),
            self._format_order_size(limited_common_qty),
            self._format_order_size(slippage_pct),
            self._format_order_size(buffer_pct),
        )
        return limited_common_qty, limited_common_qty

    def _depth20_available_qty_for_action(
        self,
        *,
        instrument: InstrumentId,
        action: str,
        slippage_pct: Decimal,
    ) -> Decimal:
        quote = self._latest_quotes.get(instrument)
        depth20 = self.market_data_service.get_depth20_snapshot(instrument)
        if quote is None or depth20 is None:
            return Decimal("0")
        side = str(action or "").strip().upper()
        if side == "BUY":
            best_price = quote.ask
            if best_price <= Decimal("0"):
                return Decimal("0")
            limit_price = best_price * (Decimal("1") + (slippage_pct / Decimal("100")))
            levels = depth20.asks
            available = Decimal("0")
            for level in levels:
                if level.price > limit_price:
                    break
                available += level.quantity
            return available
        if side == "SELL":
            best_price = quote.bid
            if best_price <= Decimal("0"):
                return Decimal("0")
            limit_price = best_price * (Decimal("1") - (slippage_pct / Decimal("100")))
            levels = depth20.bids
            available = Decimal("0")
            for level in levels:
                if level.price < limit_price:
                    break
                available += level.quantity
            return available
        return Decimal("0")

    def _entry_notional_usdt(self) -> Decimal:
        configured = self.task.target_notional
        if configured > Decimal("0"):
            return configured
        return self._decimal_or_zero(self.task.runtime_params.get("entry_notional_usdt") or self.task.runtime_params.get("test_size"))

    def _entry_cycle_notional_usdt(self) -> Decimal:
        position_cap = self._entry_notional_usdt()
        cycle_count = self._int_or_zero(self.task.runtime_params.get("cycle_count"))
        if cycle_count > 0 and position_cap > Decimal("0"):
            return (position_cap / Decimal(cycle_count)).normalize()
        cycle_notional = self.task.step_notional
        if cycle_notional > Decimal("0") and position_cap > Decimal("0"):
            return min(cycle_notional, position_cap)
        if cycle_notional > Decimal("0"):
            return cycle_notional
        return position_cap

    def _exit_cycle_notional_usdt(self) -> Decimal:
        base_notional = self._entry_cycle_notional_usdt()
        if base_notional <= Decimal("0"):
            return Decimal("0")
        return (base_notional * self._exit_cycle_growth_multiplier()).normalize()

    def _effective_entry_edge_result(self, *, edge_result: SpreadEdgeResult, threshold: Decimal, forced_signal: bool, simulated_window_open: bool) -> SpreadEdgeResult:
        if not forced_signal and not simulated_window_open:
            return edge_result
        if edge_result.best_edge is None or not edge_result.direction or not edge_result.left_action or not edge_result.right_action:
            return edge_result
        edge_name = self._edge_name_from_actions(left_action=edge_result.left_action, right_action=edge_result.right_action)
        if edge_name == "edge_1":
            return type(edge_result)(threshold, edge_result.edge_2, threshold, edge_result.direction, edge_result.left_action, edge_result.right_action)
        if edge_name == "edge_2":
            return type(edge_result)(edge_result.edge_1, threshold, threshold, edge_result.direction, edge_result.left_action, edge_result.right_action)
        return edge_result

    @staticmethod
    def _simulated_cycle_entry_edge_result(*, direction: str | None, threshold: Decimal) -> SpreadEdgeResult | None:
        from app.core.workers.runtime_core import SpreadEdgeResult

        if direction == "LEFT_SELL_RIGHT_BUY":
            return SpreadEdgeResult(threshold, None, threshold, "EDGE_1", "SELL", "BUY")
        if direction == "LEFT_BUY_RIGHT_SELL":
            return SpreadEdgeResult(None, threshold, threshold, "EDGE_2", "BUY", "SELL")
        return None

    def _take_forced_entry_signal(self) -> bool:
        requested = self._forced_entry_signal_requested
        self._forced_entry_signal_requested = False
        return requested

    def _entry_max_slippage_pct(self) -> Decimal:
        # Slippage is derived from current spread opportunity:
        # slippage_pct = clamp(abs(best_edge) * 0.2, 0.05, 0.5)
        coef = self._decimal_or_zero(self.task.runtime_params.get("slippage_from_spread_coef"))
        if coef <= Decimal("0"):
            coef = Decimal("0.2")
        min_slippage_pct = self._decimal_or_zero(self.task.runtime_params.get("min_slippage_pct"))
        if min_slippage_pct <= Decimal("0"):
            min_slippage_pct = Decimal("0.05")
        max_slippage_pct = self._decimal_or_zero(self.task.runtime_params.get("max_slippage_pct"))
        if max_slippage_pct <= Decimal("0"):
            max_slippage_pct = Decimal("0.5")
        if max_slippage_pct < min_slippage_pct:
            max_slippage_pct = min_slippage_pct
        best_edge = self._decimal_or_zero(self.state.metrics.get("best_edge"))
        spread_based_slippage = abs(best_edge) * coef
        if spread_based_slippage < min_slippage_pct:
            return min_slippage_pct
        if spread_based_slippage > max_slippage_pct:
            return max_slippage_pct
        return spread_based_slippage

    def _current_test_sizes(self, edge_result: SpreadEdgeResult) -> tuple[Decimal, Decimal]:
        legacy_left_qty, legacy_right_qty = self._configured_direct_test_sizes()
        if legacy_left_qty > Decimal("0") and legacy_right_qty > Decimal("0"):
            return legacy_left_qty, legacy_right_qty
        left_quote = self._latest_quotes.get(self._left_instrument)
        right_quote = self._latest_quotes.get(self._right_instrument)
        if left_quote is None or right_quote is None:
            return Decimal("0"), Decimal("0")
        notional = self._effective_entry_cycle_notional_usdt(edge_result)
        if notional <= Decimal("0"):
            return Decimal("0"), Decimal("0")
        common_qty = self._compute_shared_dual_leg_quantity(
            left_instrument=self._left_instrument,
            right_instrument=self._right_instrument,
            left_quote=left_quote,
            right_quote=right_quote,
            left_action=str(edge_result.left_action or ""),
            right_action=str(edge_result.right_action or ""),
            target_notional=notional,
        )
        return common_qty, common_qty

    def _configured_direct_test_sizes(self) -> tuple[Decimal, Decimal]:
        left_qty = self._decimal_or_zero(self.task.runtime_params.get("left_qty"))
        right_qty = self._decimal_or_zero(self.task.runtime_params.get("right_qty"))
        if left_qty <= Decimal("0") or right_qty <= Decimal("0"):
            return Decimal("0"), Decimal("0")
        return left_qty, right_qty

    def _entry_capacity_block_reason(self, edge_result: SpreadEdgeResult) -> str | None:
        if self.position is None:
            return None
        next_direction = (
            f"LEFT_{edge_result.left_action}_RIGHT_{edge_result.right_action}"
            if edge_result.left_action and edge_result.right_action
            else None
        )
        if next_direction is None:
            return "WAITING_QUOTES"
        next_cycle_notional = self._effective_entry_cycle_notional_usdt(edge_result)
        if self._entry_growth_limited and next_cycle_notional <= Decimal("0"):
            return "POSITION_SIZE_LIMITED_BY_MARGIN"
        if next_cycle_notional <= Decimal("0"):
            return "POSITION_CAP_REACHED"
        return None

    def _effective_entry_cycle_notional_usdt(self, edge_result: SpreadEdgeResult) -> Decimal:
        base_cycle_notional = self._entry_cycle_notional_usdt()
        if base_cycle_notional <= Decimal("0"):
            return Decimal("0")
        growth_multiplier = self._entry_cycle_growth_multiplier()
        effective_cycle_notional = (base_cycle_notional * growth_multiplier).normalize()
        growth_limit_qty = Decimal("0")
        current_qty = Decimal("0")
        remaining_qty = Decimal("0")
        if self._entry_growth_limited and self._entry_growth_limit_qty is not None and self._entry_growth_limit_qty > Decimal("0"):
            growth_limit_qty = self._entry_growth_limit_qty
            current_qty = self._current_hedged_position_qty()
            remaining_qty = max(Decimal("0"), self._entry_growth_limit_qty - current_qty)
            if remaining_qty <= Decimal("0"):
                self._maybe_log_entry_cycle_clamp(
                    reason="MARGIN_LIMIT_QTY_REACHED",
                    base_cycle_notional=base_cycle_notional,
                    effective_cycle_notional=Decimal("0"),
                    position_cap_notional=self._effective_entry_position_cap_notional_usdt(),
                    current_position_notional=self._current_position_notional_usdt(edge_result),
                    remaining_notional=Decimal("0"),
                    growth_limit_qty=growth_limit_qty,
                    current_qty=current_qty,
                    remaining_qty=remaining_qty,
                )
                return Decimal("0")
            left_quote = self._latest_quotes.get(self._left_instrument)
            right_quote = self._latest_quotes.get(self._right_instrument)
            if left_quote is not None and right_quote is not None:
                left_action = str(edge_result.left_action or "")
                right_action = str(edge_result.right_action or "")
                left_reference_price = self._reference_price_for_action(quote=left_quote, action=left_action) if left_action in {"BUY", "SELL"} else Decimal("0")
                right_reference_price = self._reference_price_for_action(quote=right_quote, action=right_action) if right_action in {"BUY", "SELL"} else Decimal("0")
                expensive_price = max(left_reference_price, right_reference_price)
                if expensive_price > Decimal("0"):
                    effective_cycle_notional = min(effective_cycle_notional, (remaining_qty * expensive_price).normalize())
        position_cap = self._effective_entry_position_cap_notional_usdt()
        if position_cap <= Decimal("0"):
            if effective_cycle_notional < base_cycle_notional:
                self._maybe_log_entry_cycle_clamp(
                    reason="MARGIN_LIMIT_QTY_CEILING",
                    base_cycle_notional=base_cycle_notional,
                    effective_cycle_notional=effective_cycle_notional,
                    position_cap_notional=position_cap,
                    current_position_notional=self._current_position_notional_usdt(edge_result),
                    remaining_notional=Decimal("0"),
                    growth_limit_qty=growth_limit_qty,
                    current_qty=current_qty,
                    remaining_qty=remaining_qty,
                )
            return effective_cycle_notional
        current_position_notional = self._current_position_notional_usdt(edge_result)
        reserved_inflight_notional = self._reserved_entry_inflight_notional_usdt()
        remaining_notional = max(Decimal("0"), position_cap - current_position_notional - reserved_inflight_notional)
        if remaining_notional <= Decimal("0"):
            self._maybe_log_entry_cycle_clamp(
                reason="POSITION_CAP_REACHED",
                base_cycle_notional=base_cycle_notional,
                effective_cycle_notional=Decimal("0"),
                position_cap_notional=position_cap,
                current_position_notional=current_position_notional,
                remaining_notional=remaining_notional,
                growth_limit_qty=growth_limit_qty,
                current_qty=current_qty,
                remaining_qty=remaining_qty,
            )
            return Decimal("0")
        effective_cycle_notional = min(effective_cycle_notional, remaining_notional)
        # Hard cap guard by worst-case quantity projection (current + in-flight + next cycle).
        # This prevents overshoot when stale/incomplete cycle notionals under-report reserved exposure.
        left_quote = self._latest_quotes.get(self._left_instrument)
        right_quote = self._latest_quotes.get(self._right_instrument)
        left_action = str(edge_result.left_action or "").strip().upper()
        right_action = str(edge_result.right_action or "").strip().upper()
        if (
            left_quote is not None
            and right_quote is not None
            and left_action in {"BUY", "SELL"}
            and right_action in {"BUY", "SELL"}
        ):
            left_reference_price = self._reference_price_for_action(quote=left_quote, action=left_action)
            right_reference_price = self._reference_price_for_action(quote=right_quote, action=right_action)
            expensive_price = max(left_reference_price, right_reference_price)
            if expensive_price > Decimal("0"):
                cap_multiplier = self._entry_cap_price_buffer_multiplier()
                conservative_expensive_price = (expensive_price * cap_multiplier).normalize()
                cap_qty = position_cap / conservative_expensive_price if conservative_expensive_price > Decimal("0") else Decimal("0")
                current_open_qty = max(self.left_leg_state.filled_qty, self.right_leg_state.filled_qty)
                reserved_inflight_qty = self._reserved_entry_inflight_qty()
                remaining_cap_qty = max(Decimal("0"), cap_qty - current_open_qty - reserved_inflight_qty)
                common_step = self._common_step_size(self._left_instrument.spec.qty_precision, self._right_instrument.spec.qty_precision)
                if common_step > Decimal("0"):
                    remaining_cap_qty = (remaining_cap_qty / common_step).to_integral_value(rounding=ROUND_DOWN) * common_step
                max_notional_by_qty = (remaining_cap_qty * expensive_price).normalize()
                effective_cycle_notional = min(effective_cycle_notional, max_notional_by_qty)
        minimum_cycle_notional = self._minimum_entry_cycle_notional_usdt(edge_result)
        if minimum_cycle_notional > Decimal("0") and effective_cycle_notional < minimum_cycle_notional:
            # Remaining notional is below exchange executable step/notional floor.
            # Treat target as reached and stop further entry attempts.
            effective_cycle_notional = Decimal("0")
        if effective_cycle_notional < base_cycle_notional:
            clamp_reason = "POSITION_CAP_REMAINDER"
            if growth_limit_qty > Decimal("0") and effective_cycle_notional < remaining_notional:
                clamp_reason = "MARGIN_LIMIT_QTY_CEILING"
            elif remaining_notional > Decimal("0") and minimum_cycle_notional > Decimal("0") and remaining_notional < minimum_cycle_notional:
                clamp_reason = "POSITION_CAP_BELOW_EXCHANGE_MIN_STEP"
            self._maybe_log_entry_cycle_clamp(
                reason=clamp_reason,
                base_cycle_notional=base_cycle_notional,
                effective_cycle_notional=effective_cycle_notional,
                position_cap_notional=position_cap,
                current_position_notional=current_position_notional,
                remaining_notional=remaining_notional,
                growth_limit_qty=growth_limit_qty,
                current_qty=current_qty,
                remaining_qty=remaining_qty,
            )
        return effective_cycle_notional

    def _minimum_entry_cycle_notional_usdt(self, edge_result: SpreadEdgeResult) -> Decimal:
        left_quote = self._latest_quotes.get(self._left_instrument)
        right_quote = self._latest_quotes.get(self._right_instrument)
        left_action = str(edge_result.left_action or "").strip().upper()
        right_action = str(edge_result.right_action or "").strip().upper()
        if (
            left_quote is None
            or right_quote is None
            or left_action not in {"BUY", "SELL"}
            or right_action not in {"BUY", "SELL"}
        ):
            return Decimal("0")
        left_reference_price = self._reference_price_for_action(quote=left_quote, action=left_action)
        right_reference_price = self._reference_price_for_action(quote=right_quote, action=right_action)
        expensive_price = max(left_reference_price, right_reference_price)
        min_common_qty = max(self._left_instrument.spec.min_qty, self._right_instrument.spec.min_qty)
        qty_floor_notional = (min_common_qty * expensive_price).normalize() if min_common_qty > Decimal("0") and expensive_price > Decimal("0") else Decimal("0")
        exchange_notional_floor = max(
            self._decimal_or_zero(self._left_instrument.spec.min_notional),
            self._decimal_or_zero(self._right_instrument.spec.min_notional),
        )
        return max(qty_floor_notional, exchange_notional_floor)

    def _reserved_entry_inflight_notional_usdt(self) -> Decimal:
        reserved = Decimal("0")
        for cycle in (self.active_entry_cycle, self.prefetch_entry_cycle):
            if cycle is None:
                continue
            if cycle.target_notional_usdt > Decimal("0"):
                reserved += cycle.target_notional_usdt
        return reserved

    def _reserved_entry_inflight_qty(self) -> Decimal:
        reserved = Decimal("0")
        for cycle in (self.active_entry_cycle, self.prefetch_entry_cycle):
            if cycle is None:
                continue
            left_target_qty = self._decimal_or_zero(getattr(cycle, "left_target_qty", Decimal("0")))
            right_target_qty = self._decimal_or_zero(getattr(cycle, "right_target_qty", Decimal("0")))
            reserved += max(left_target_qty, right_target_qty)
        return reserved

    def _entry_cap_price_buffer_multiplier(self) -> Decimal:
        try:
            buffer_pct = Decimal(str(self.task.runtime_params.get("entry_cap_price_buffer_pct") or "1"))
        except Exception:
            buffer_pct = Decimal("1")
        if buffer_pct < Decimal("0"):
            buffer_pct = Decimal("0")
        return Decimal("1") + (buffer_pct / Decimal("100"))

    def _entry_cycle_growth_multiplier(self) -> Decimal:
        try:
            base = Decimal(str(self.task.runtime_params.get("entry_cycle_growth_factor") or "1.3"))
        except Exception:
            base = Decimal("1.3")
        if base < Decimal("1"):
            base = Decimal("1")
        streak = int(getattr(self, "_entry_cycle_success_streak", 0) or 0)
        if streak <= 0:
            return Decimal("1")
        return base ** streak

    def _exit_cycle_growth_multiplier(self) -> Decimal:
        try:
            base = Decimal(str(self.task.runtime_params.get("exit_cycle_growth_factor") or "1.3"))
        except Exception:
            base = Decimal("1.3")
        if base < Decimal("1"):
            base = Decimal("1")
        streak = int(getattr(self, "_exit_cycle_success_streak", 0) or 0)
        if streak <= 0:
            return Decimal("1")
        return base ** streak

    def _effective_entry_position_cap_notional_usdt(self) -> Decimal:
        configured_cap = self._entry_notional_usdt()
        if configured_cap <= Decimal("0"):
            return configured_cap
        if self._entry_growth_limited and self._entry_growth_limit_notional_usdt is not None and self._entry_growth_limit_notional_usdt > Decimal("0"):
            return min(configured_cap, self._entry_growth_limit_notional_usdt)
        return configured_cap

    def _current_position_notional_usdt(self, edge_result: SpreadEdgeResult) -> Decimal:
        if self.position is None:
            return Decimal("0")
        left_quote = self._latest_quotes.get(self._left_instrument)
        right_quote = self._latest_quotes.get(self._right_instrument)
        if left_quote is None or right_quote is None:
            return Decimal("0")
        left_action = str(edge_result.left_action or self.position.left_side or "")
        right_action = str(edge_result.right_action or self.position.right_side or "")
        left_reference_price = self._reference_price_for_action(quote=left_quote, action=left_action) if left_action in {"BUY", "SELL"} else Decimal("0")
        right_reference_price = self._reference_price_for_action(quote=right_quote, action=right_action) if right_action in {"BUY", "SELL"} else Decimal("0")
        if left_reference_price <= Decimal("0") or right_reference_price <= Decimal("0"):
            return Decimal("0")
        # Use worst-case open exposure for cap checks so overlap drift
        # cannot push a single leg beyond the configured position limit.
        common_open_qty = max(self.left_leg_state.filled_qty, self.right_leg_state.filled_qty)
        expensive_price = max(left_reference_price, right_reference_price)
        return (common_open_qty * expensive_price).normalize()

    def _compute_shared_dual_leg_quantity(
        self,
        *,
        left_instrument: InstrumentId,
        right_instrument: InstrumentId,
        left_quote: QuoteL1,
        right_quote: QuoteL1,
        left_action: str,
        right_action: str,
        target_notional: Decimal,
    ) -> Decimal:
        normalized_left_action = str(left_action or "").strip().upper()
        normalized_right_action = str(right_action or "").strip().upper()
        if normalized_left_action not in {"BUY", "SELL"} or normalized_right_action not in {"BUY", "SELL"} or target_notional <= Decimal("0"):
            return Decimal("0")
        left_reference_price = self._reference_price_for_action(quote=left_quote, action=normalized_left_action)
        right_reference_price = self._reference_price_for_action(quote=right_quote, action=normalized_right_action)
        expensive_leg = "left" if left_reference_price >= right_reference_price else "right"
        expensive_price = left_reference_price if expensive_leg == "left" else right_reference_price
        common_step_size = self._common_step_size(left_instrument.spec.qty_precision, right_instrument.spec.qty_precision)
        common_min_qty = max(left_instrument.spec.min_qty, right_instrument.spec.min_qty)
        try:
            quantity = self._compute_order_quantity(
                target_notional=target_notional,
                reference_price=expensive_price,
                step_size=common_step_size,
                min_qty=common_min_qty,
            )
            signature = (
                self._format_order_size(target_notional),
                expensive_leg,
                self._format_order_size(expensive_price),
                self._format_order_size(quantity),
                self._format_order_size(left_instrument.spec.qty_precision),
                self._format_order_size(right_instrument.spec.qty_precision),
                self._format_order_size(common_step_size if common_step_size > Decimal("0") else Decimal("0")),
            )
            now_ms = int(time.time() * 1000)
            if (
                signature != self._last_entry_sizing_log_signature
                or (now_ms - self._last_entry_sizing_log_at_ms) >= 3000
            ):
                self._last_entry_sizing_log_signature = signature
                self._last_entry_sizing_log_at_ms = now_ms
                self.logger.debug(
                    "entry sizing | mode=shared_qty_expensive_leg | target_notional_usdt=%s | expensive_leg=%s | expensive_price=%s | common_qty=%s | left_step=%s | right_step=%s | common_step=%s",
                    signature[0],
                    signature[1],
                    signature[2],
                    signature[3],
                    signature[4],
                    signature[5],
                    signature[6],
                )
            return quantity
        except Exception:
            return Decimal("0")

    @staticmethod
    def _reference_price_for_action(*, quote: QuoteL1, action: str) -> Decimal:
        return quote.ask if action == "BUY" else quote.bid

    @staticmethod
    def _common_step_size(*step_sizes: Decimal) -> Decimal:
        positive_steps = [step.normalize() for step in step_sizes if step is not None and step > Decimal("0")]
        if not positive_steps:
            return Decimal("0")
        scale = max(max(0, -step.as_tuple().exponent) for step in positive_steps)
        factor = 10 ** scale
        scaled_steps = [int((step * factor).to_integral_value()) for step in positive_steps]
        common_scaled_step = scaled_steps[0]
        for scaled_step in scaled_steps[1:]:
            common_scaled_step = (common_scaled_step * scaled_step) // gcd(common_scaled_step, scaled_step)
        return (Decimal(common_scaled_step) / Decimal(factor)).normalize()

    @staticmethod
    def _format_order_size(value: Decimal) -> str:
        return format(value.normalize(), "f")

    @staticmethod
    def _compute_order_quantity(*, target_notional: Decimal, reference_price: Decimal, step_size: Decimal, min_qty: Decimal) -> Decimal:
        if reference_price <= Decimal("0"):
            raise RuntimeError("Reference price must be positive")
        raw_quantity = target_notional / reference_price
        if step_size > Decimal("0"):
            steps = (raw_quantity / step_size).to_integral_value(rounding=ROUND_DOWN)
            quantity = steps * step_size
        else:
            quantity = raw_quantity
        if min_qty > Decimal("0") and quantity < min_qty:
            quantity = min_qty
        if quantity <= Decimal("0"):
            raise RuntimeError("Computed order quantity is zero")
        return quantity.normalize()

    @staticmethod
    def _round_price_to_tick(*, price: Decimal, tick_size: Decimal, rounding_mode: str) -> Decimal:
        if tick_size <= Decimal("0"):
            return price.normalize()
        steps = (price / tick_size).to_integral_value(rounding=rounding_mode)
        rounded_price = steps * tick_size
        if rounded_price <= Decimal("0"):
            raise RuntimeError("Rounded price is zero")
        return rounded_price.normalize()

    @staticmethod
    def _decimal_or_none(value: Any) -> Decimal | None:
        if value in (None, "", "-"):
            return None
        try:
            return Decimal(str(value).strip())
        except Exception:
            return None

    @staticmethod
    def _decimal_or_zero(value: Any) -> Decimal:
        parsed = WorkerRuntimeSizingMixin._decimal_or_none(value)
        return parsed if parsed is not None else Decimal("0")

    @staticmethod
    def _int_or_zero(value: Any) -> int:
        try:
            return int(str(value).strip())
        except Exception:
            return 0
