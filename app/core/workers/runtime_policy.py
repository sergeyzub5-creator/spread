from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

from app.core.models.workers import EntryDecision
from app.core.workers.runtime_entry_decision_flow import build_entry_decision as build_entry_decision_flow
from app.core.workers.runtime_exit_orchestrator import build_exit_decision as build_exit_decision_flow

if TYPE_CHECKING:
    from app.core.workers.runtime_core import WorkerRuntime


class RuntimePolicy(Protocol):
    name: str

    def allow_entry_evaluation(self, runtime: WorkerRuntime) -> bool:
        ...

    def allow_exit_evaluation(self, runtime: WorkerRuntime) -> bool:
        ...

    def build_entry_decision(self, runtime: WorkerRuntime) -> EntryDecision | None:
        ...

    def build_exit_decision(self, runtime: WorkerRuntime) -> dict[str, Any] | None:
        ...


class NewRuntimePolicy:
    """Primary policy branch.

    Entry and exit policies are delegated to their dedicated decision flows.
    """

    name = "new"

    def _log_bootstrap_once(self, runtime: WorkerRuntime) -> None:
        if bool(getattr(runtime, "_new_policy_bootstrap_logged", False)):
            return
        runtime._new_policy_bootstrap_logged = True
        runtime.logger.info(
            "runtime policy '%s' active | entry and exit policies enabled",
            self.name,
        )

    def allow_entry_evaluation(self, runtime: WorkerRuntime) -> bool:
        self._log_bootstrap_once(runtime)
        return True

    def allow_exit_evaluation(self, runtime: WorkerRuntime) -> bool:
        self._log_bootstrap_once(runtime)
        return True

    def build_entry_decision(self, runtime: WorkerRuntime) -> EntryDecision | None:
        # Reuse the dedicated entry flow that already handles simulated windows,
        # cooldown, pipeline guards, capacity checks and validation.
        return build_entry_decision_flow(runtime)

    def build_exit_decision(self, runtime: WorkerRuntime) -> dict[str, Any] | None:
        return build_exit_decision_flow(runtime)


def create_runtime_policy(policy_name: str) -> RuntimePolicy:
    return NewRuntimePolicy()
