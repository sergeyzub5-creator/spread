from __future__ import annotations

import unittest

from app.core.workers.runtime_guard_mixin import WorkerRuntimeGuardMixin


class _FakeLogger:
    def info(self, *_args, **_kwargs) -> None:
        return


class _FakeRuntime(WorkerRuntimeGuardMixin):
    def __init__(self, owner: str | None) -> None:
        self._owner = owner
        self.logger = _FakeLogger()

    def _current_owner_context(self) -> str | None:
        return self._owner


class RuntimeReconcilePreemptionTests(unittest.TestCase):
    def test_invariant_idle_with_active_cycles_can_preempt_owner(self) -> None:
        runtime = _FakeRuntime("ENTRY_CYCLE#8")
        self.assertTrue(
            runtime._runtime_reconcile_can_preempt_active_owner(
                reason="INVARIANT_IDLE_WITH_ACTIVE_CYCLES"
            )
        )

    def test_regular_reason_still_deferred_with_owner(self) -> None:
        runtime = _FakeRuntime("ENTRY_CYCLE#8")
        self.assertFalse(runtime._runtime_reconcile_can_preempt_active_owner(reason="PERIODIC_FULL_STATE"))


if __name__ == "__main__":
    unittest.main()
