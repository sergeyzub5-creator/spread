from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from app.core.models.market_data import QuoteL1


@dataclass(frozen=True, slots=True)
class EntryValidationResult:
    is_valid: bool
    block_reason: str | None
    left_valid: bool
    right_valid: bool
    liquidity_ok: bool
    fresh_ok: bool
    left_liquidity_ok: bool
    right_liquidity_ok: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "is_valid": self.is_valid,
            "block_reason": self.block_reason,
            "left_valid": self.left_valid,
            "right_valid": self.right_valid,
            "liquidity_ok": self.liquidity_ok,
            "fresh_ok": self.fresh_ok,
            "left_liquidity_ok": self.left_liquidity_ok,
            "right_liquidity_ok": self.right_liquidity_ok,
        }


class SpreadEntryValidator:
    def __init__(self, *, freshness_threshold_ms: int = 2500) -> None:
        self._freshness_threshold_ms = max(0, int(freshness_threshold_ms))

    def validate_entry(
        self,
        *,
        left_quote: QuoteL1,
        right_quote: QuoteL1,
        left_action: str,
        right_action: str,
        left_test_size: Decimal,
        right_test_size: Decimal,
        left_quote_age_ms: int | None,
        right_quote_age_ms: int | None,
        max_quote_skew_ms: int | None = None,
        enforce_liquidity: bool = True,
    ) -> EntryValidationResult:
        left_valid, left_reason = self._validate_quote(left_quote, "LEFT")
        right_valid, right_reason = self._validate_quote(right_quote, "RIGHT")
        liquidity_ok, liquidity_reason, left_liquidity_ok, right_liquidity_ok = self._validate_liquidity(
            left_quote=left_quote,
            right_quote=right_quote,
            left_action=left_action,
            right_action=right_action,
            left_test_size=left_test_size,
            right_test_size=right_test_size,
        )
        if not left_valid:
            return EntryValidationResult(False, left_reason, False, right_valid, liquidity_ok, False, left_liquidity_ok, right_liquidity_ok)
        if not right_valid:
            return EntryValidationResult(False, right_reason, True, False, liquidity_ok, False, left_liquidity_ok, right_liquidity_ok)
        if left_test_size <= Decimal("0") or right_test_size <= Decimal("0"):
            return EntryValidationResult(False, "INVALID_TEST_SIZE", True, True, False, False, left_liquidity_ok, right_liquidity_ok)

        fresh_ok, fresh_reason = self._validate_freshness(
            left_quote=left_quote,
            right_quote=right_quote,
            left_quote_age_ms=left_quote_age_ms,
            right_quote_age_ms=right_quote_age_ms,
            max_quote_skew_ms=max_quote_skew_ms,
        )
        if not fresh_ok:
            return EntryValidationResult(False, fresh_reason, True, True, liquidity_ok, False, left_liquidity_ok, right_liquidity_ok)
        if enforce_liquidity and not liquidity_ok:
            return EntryValidationResult(False, liquidity_reason, True, True, False, True, left_liquidity_ok, right_liquidity_ok)

        return EntryValidationResult(True, None, True, True, (liquidity_ok if enforce_liquidity else True), True, left_liquidity_ok, right_liquidity_ok)

    @staticmethod
    def _validate_quote(quote: QuoteL1, prefix: str) -> tuple[bool, str | None]:
        if quote.bid <= Decimal("0") or quote.ask <= Decimal("0"):
            return False, f"{prefix}_INVALID_PRICE"
        if quote.bid >= quote.ask:
            return False, f"{prefix}_CROSSED_BOOK"
        if quote.bid_qty <= Decimal("0") or quote.ask_qty <= Decimal("0"):
            return False, f"{prefix}_INVALID_QTY"
        return True, None

    def _validate_freshness(
        self,
        *,
        left_quote: QuoteL1,
        right_quote: QuoteL1,
        left_quote_age_ms: int | None,
        right_quote_age_ms: int | None,
        max_quote_skew_ms: int | None,
    ) -> tuple[bool, str | None]:
        left_age = self._coerce_age(left_quote_age_ms, left_quote)
        right_age = self._coerce_age(right_quote_age_ms, right_quote)
        if left_age > self._freshness_threshold_ms:
            return False, "LEFT_STALE_QUOTE"
        if right_age > self._freshness_threshold_ms:
            return False, "RIGHT_STALE_QUOTE"
        return True, None

    @staticmethod
    def _validate_liquidity(
        *,
        left_quote: QuoteL1,
        right_quote: QuoteL1,
        left_action: str,
        right_action: str,
        left_test_size: Decimal,
        right_test_size: Decimal,
    ) -> tuple[bool, str | None, bool, bool]:
        left_available = left_quote.ask_qty if str(left_action or "").strip().upper() == "BUY" else left_quote.bid_qty
        right_available = right_quote.ask_qty if str(right_action or "").strip().upper() == "BUY" else right_quote.bid_qty
        left_liquidity_ok = left_available >= left_test_size if left_test_size > Decimal("0") else False
        right_liquidity_ok = right_available >= right_test_size if right_test_size > Decimal("0") else False
        if not left_liquidity_ok or not right_liquidity_ok:
            return False, "INSUFFICIENT_TOP_QTY", left_liquidity_ok, right_liquidity_ok
        return True, None, True, True

    @staticmethod
    def _coerce_age(value: int | None, quote: QuoteL1) -> int:
        if value is not None:
            return max(0, int(value))
        return max(0, int(quote.ts_local))
