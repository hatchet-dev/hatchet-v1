import asyncio
from typing import Any, Awaitable, Callable, Generic, TypeGuard, Union

from hatchet_sdk.context.context import Context
from hatchet_sdk.contracts.workflows_pb2 import CreateStepRateLimit, DesiredWorkerLabels
from hatchet_sdk.runnables.types import ConcurrencyLimitStrategy, R, StepType
from hatchet_sdk.runnables.workflow import BaseWorkflow

AsyncFunc = Callable[[Any, Context], Awaitable[R]]
SyncFunc = Callable[[Any, Context], R]
TaskFunc = Union[AsyncFunc[R], SyncFunc[R]]


def is_async_fn(fn: TaskFunc[R]) -> TypeGuard[AsyncFunc[R]]:
    return asyncio.iscoroutinefunction(fn)


def is_sync_fn(fn: TaskFunc[R]) -> TypeGuard[SyncFunc[R]]:
    return not asyncio.iscoroutinefunction(fn)


class Task(Generic[R]):
    def __init__(
        self,
        fn: Callable[[Any, Context], R] | Callable[[Any, Context], Awaitable[R]],
        type: StepType,
        name: str = "",
        timeout: str = "60m",
        parents: list[str] = [],
        retries: int = 0,
        rate_limits: list[CreateStepRateLimit] = [],
        desired_worker_labels: dict[str, DesiredWorkerLabels] = {},
        backoff_factor: float | None = None,
        backoff_max_seconds: int | None = None,
        concurrency__max_runs: int | None = None,
        concurrency__limit_strategy: ConcurrencyLimitStrategy | None = None,
    ) -> None:
        self.fn = fn
        self.is_async_function = is_async_fn(fn)
        self.workflow: Union["BaseWorkflow", None] = None

        self.type = type
        self.timeout = timeout
        self.name = name
        self.parents = parents
        self.retries = retries
        self.rate_limits = rate_limits
        self.desired_worker_labels = desired_worker_labels
        self.backoff_factor = backoff_factor
        self.backoff_max_seconds = backoff_max_seconds
        self.concurrency__max_runs = concurrency__max_runs
        self.concurrency__limit_strategy = concurrency__limit_strategy

    def call(self, ctx: Context) -> R:
        if not self.is_registered:
            raise ValueError(
                "Only steps that have been registered can be called. To register this step, instantiate its corresponding workflow."
            )

        if self.is_async_function:
            raise TypeError(f"{self.name} is not a sync function. Use `acall` instead.")

        sync_fn = self.fn
        if is_sync_fn(sync_fn):
            return sync_fn(self.workflow, ctx)

        raise TypeError(f"{self.name} is not a sync function. Use `acall` instead.")

    async def aio_call(self, ctx: Context) -> R:
        if not self.is_registered:
            raise ValueError(
                "Only steps that have been registered can be called. To register this step, instantiate its corresponding workflow."
            )

        if not self.is_async_function:
            raise TypeError(
                f"{self.name} is not an async function. Use `call` instead."
            )

        async_fn = self.fn

        if is_async_fn(async_fn):
            return await async_fn(self.workflow, ctx)

        raise TypeError(f"{self.name} is not an async function. Use `call` instead.")

    @property
    def is_registered(self) -> bool:
        return self.workflow is not None
