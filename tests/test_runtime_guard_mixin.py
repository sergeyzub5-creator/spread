from __future__ import annotations

import unittest

from app.core.models.workers import StrategyState
from app.core.workers.runtime_guard_mixin import WorkerRuntimeGuardMixin


class _FakeGuardRuntime(WorkerRuntimeGuardMixin):
    def __init__(self, strategy_state: StrategyState, *, runtime_reconcile_active: bool = False) -> None:
        self.strategy_state = strategy_state
        self._runtime_reconcile_active_flag = runtime_reconcile_active

    def _runtime_reconcile_active(self) -> bool:
        return self._runtime_reconcile_active_flag


class RuntimeGuardMixinTests(unittest.TestCase):
    def test_strategy_allows_hedge_protection_in_idle(self) -> None:
        runtime = _FakeGuardRuntime(StrategyState.IDLE)
        self.assertTrue(runtime._strategy_allows_hedge_protection())

    def test_strategy_disallows_hedge_protection_during_reconcile(self) -> None:
        runtime = _FakeGuardRuntime(StrategyState.IDLE, runtime_reconcile_active=True)
        self.assertFalse(runtime._strategy_allows_hedge_protection())


if __name__ == "__main__":
    unittest.main()
