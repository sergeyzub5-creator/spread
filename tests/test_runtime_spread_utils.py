from __future__ import annotations

import unittest
from decimal import Decimal
from types import SimpleNamespace

from app.core.workers.runtime_spread_utils import calculate_spread_edges


class RuntimeSpreadUtilsTests(unittest.TestCase):
    def test_selects_edge_by_abs_magnitude_when_both_negative(self) -> None:
        left_quote = SimpleNamespace(bid=Decimal("70"), ask=Decimal("100"))
        right_quote = SimpleNamespace(bid=Decimal("90"), ask=Decimal("100"))

        result = calculate_spread_edges(left_quote, right_quote)

        self.assertEqual(result.direction, "EDGE_1")
        self.assertEqual(result.left_action, "SELL")
        self.assertEqual(result.right_action, "BUY")
        self.assertEqual(result.best_edge, result.edge_1)

    def test_selects_edge_by_abs_magnitude_when_signs_mixed(self) -> None:
        left_quote = SimpleNamespace(bid=Decimal("100"), ask=Decimal("101"))
        right_quote = SimpleNamespace(bid=Decimal("80"), ask=Decimal("95"))

        result = calculate_spread_edges(left_quote, right_quote)

        self.assertEqual(result.direction, "EDGE_2")
        self.assertEqual(result.left_action, "BUY")
        self.assertEqual(result.right_action, "SELL")
        self.assertEqual(result.best_edge, result.edge_2)


if __name__ == "__main__":
    unittest.main()
