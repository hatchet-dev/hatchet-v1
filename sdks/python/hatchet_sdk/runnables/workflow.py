import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeGuard, Union, cast

from google.protobuf import timestamp_pb2

from hatchet_sdk.clients.admin import (
    ChildTriggerWorkflowOptions,
    ChildWorkflowRunDict,
    ScheduleTriggerWorkflowOptions,
)
from hatchet_sdk.context.context import Context
from hatchet_sdk.contracts.workflows_pb2 import (
    ConcurrencyLimitStrategy as ConcurrencyLimitStrategyProto,
)
from hatchet_sdk.contracts.workflows_pb2 import (
    CreateWorkflowJobOpts,
    CreateWorkflowStepOpts,
    CreateWorkflowVersionOpts,
)
from hatchet_sdk.contracts.workflows_pb2 import StickyStrategy as StickyStrategyProto
from hatchet_sdk.contracts.workflows_pb2 import (
    WorkflowConcurrencyOpts,
    WorkflowKind,
    WorkflowVersion,
)
from hatchet_sdk.labels import DesiredWorkerLabel, transform_desired_worker_label
from hatchet_sdk.logger import logger
from hatchet_sdk.rate_limit import RateLimit
from hatchet_sdk.runnables.task import Task
from hatchet_sdk.runnables.types import (
    AsyncFunc,
    R,
    StepFunc,
    StepType,
    SyncFunc,
    TWorkflowInput,
    WorkflowConfig,
)
from hatchet_sdk.utils.proto_enums import convert_python_enum_to_proto, maybe_int_to_str
from hatchet_sdk.workflow_run import WorkflowRunRef

if TYPE_CHECKING:
    from hatchet_sdk import Hatchet


def is_async_fn(fn: StepFunc[R]) -> TypeGuard[AsyncFunc[R]]:
    return asyncio.iscoroutinefunction(fn)


def is_sync_fn(fn: StepFunc[R]) -> TypeGuard[SyncFunc[R]]:
    return not asyncio.iscoroutinefunction(fn)


@dataclass
class SpawnWorkflowInput(Generic[TWorkflowInput]):
    input: TWorkflowInput
    key: str | None = None
    options: ChildTriggerWorkflowOptions = field(
        default_factory=ChildTriggerWorkflowOptions
    )


class WorkflowDeclaration(Generic[TWorkflowInput]):
    def __init__(self, config: WorkflowConfig, hatchet: Union["Hatchet", None]):
        self.config = config
        self.hatchet = hatchet

    def run(self, input: TWorkflowInput | None = None) -> WorkflowRunRef:
        if not self.hatchet:
            raise ValueError("Hatchet client is not initialized.")

        return self.hatchet.admin.run_workflow(
            workflow_name=self.config.name, input=input.model_dump() if input else {}
        )

    def get_workflow_input(self, ctx: Context) -> TWorkflowInput:
        return cast(
            TWorkflowInput,
            self.config.input_validator.model_validate(ctx.workflow_input),
        )

    async def aio_spawn_many(
        self, ctx: Context, spawn_inputs: list[SpawnWorkflowInput[TWorkflowInput]]
    ) -> list[WorkflowRunRef]:
        inputs = [
            ChildWorkflowRunDict(
                workflow_name=self.config.name,
                input=spawn_input.input.model_dump(),
                key=spawn_input.key,
                options=spawn_input.options,
            )
            for spawn_input in spawn_inputs
        ]
        return await ctx.aio_spawn_workflows(inputs)

    async def aio_spawn_one(
        self,
        ctx: Context,
        input: TWorkflowInput,
        key: str | None = None,
        options: ChildTriggerWorkflowOptions = ChildTriggerWorkflowOptions(),
    ) -> WorkflowRunRef:
        return await ctx.aio_spawn_workflow(
            workflow_name=self.config.name,
            input=input.model_dump(),
            key=key,
            options=options,
        )

    def spawn_many(
        self, ctx: Context, spawn_inputs: list[SpawnWorkflowInput[TWorkflowInput]]
    ) -> list[WorkflowRunRef]:
        inputs = [
            ChildWorkflowRunDict(
                workflow_name=self.config.name,
                input=spawn_input.input.model_dump(),
                key=spawn_input.key,
                options=spawn_input.options,
            )
            for spawn_input in spawn_inputs
        ]

        return ctx.spawn_workflows(inputs)

    def spawn_one(
        self,
        ctx: Context,
        input: TWorkflowInput,
        key: str | None = None,
        options: ChildTriggerWorkflowOptions = ChildTriggerWorkflowOptions(),
    ) -> WorkflowRunRef:
        return ctx.spawn_workflow(
            workflow_name=self.config.name,
            input=input.model_dump(),
            key=key,
            options=options,
        )

    def schedule(
        self,
        schedules: list[datetime | timestamp_pb2.Timestamp],
        input: TWorkflowInput,
        options: ScheduleTriggerWorkflowOptions = ScheduleTriggerWorkflowOptions(),
    ) -> WorkflowVersion:
        if not self.hatchet:
            raise ValueError("Hatchet client is not initialized.")

        return self.hatchet.admin.schedule_workflow(
            name=self.config.name,
            schedules=schedules,
            input=input.model_dump(),
            options=options,
        )

    async def aio_schedule(
        self,
        schedules: list[datetime | timestamp_pb2.Timestamp],
        input: TWorkflowInput,
        options: ScheduleTriggerWorkflowOptions = ScheduleTriggerWorkflowOptions(),
    ) -> WorkflowVersion:
        if not self.hatchet:
            raise ValueError("Hatchet client is not initialized.")

        return await self.hatchet.admin.aio_schedule_workflow(
            name=self.config.name,
            schedules=schedules,
            input=input.model_dump(),
            options=options,
        )

    def task(
        self,
        name: str = "",
        timeout: str = "60m",
        parents: list[str] = [],
        retries: int = 0,
        rate_limits: list[RateLimit] = [],
        desired_worker_labels: dict[str, DesiredWorkerLabel] = {},
        backoff_factor: float | None = None,
        backoff_max_seconds: int | None = None,
    ) -> Callable[[Callable[[Any, Context], R]], Task[R]]:
        def inner(func: Callable[[Any, Context], R]) -> Task[R]:
            return Task(
                fn=func,
                type=StepType.DEFAULT,
                name=name.lower() or str(func.__name__).lower(),
                timeout=timeout,
                parents=parents,
                retries=retries,
                rate_limits=[r for rate_limit in rate_limits if (r := rate_limit._req)],
                desired_worker_labels={
                    key: transform_desired_worker_label(d)
                    for key, d in desired_worker_labels.items()
                },
                backoff_factor=backoff_factor,
                backoff_max_seconds=backoff_max_seconds,
            )

        return inner

    def on_failure_task(
        self,
        name: str = "",
        timeout: str = "60m",
        parents: list[str] = [],
        retries: int = 0,
        rate_limits: list[RateLimit] = [],
        desired_worker_labels: dict[str, DesiredWorkerLabel] = {},
        backoff_factor: float | None = None,
        backoff_max_seconds: int | None = None,
    ) -> Callable[[Callable[[Any, Context], R]], Task[R]]:
        def inner(func: Callable[[Any, Context], R]) -> Task[R]:
            return Task(
                fn=func,
                type=StepType.ON_FAILURE,
                name=name.lower() or str(func.__name__).lower(),
                timeout=timeout,
                parents=parents,
                retries=retries,
                rate_limits=[r for rate_limit in rate_limits if (r := rate_limit._req)],
                desired_worker_labels={
                    key: transform_desired_worker_label(d)
                    for key, d in desired_worker_labels.items()
                },
                backoff_factor=backoff_factor,
                backoff_max_seconds=backoff_max_seconds,
            )

        return inner


class BaseWorkflow:
    """
    A Hatchet workflow implementation base. This class should be inherited by all workflow implementations.

    A declaration is passed to the workflow using the `declaration` parameter. This declaration is used to
    define the workflow's configuration.
    """

    def __init__(self, declaration: WorkflowDeclaration[TWorkflowInput]) -> None:
        self.config = declaration.config
        self.config.name = self.config.name or str(self.__class__.__name__)

        for step in self.steps:
            step.workflow = self

    def get_service_name(self, namespace: str) -> str:
        return f"{namespace}{self.config.name.lower()}"

    def _get_steps_by_type(self, step_type: StepType) -> list[Task[Any]]:
        return [
            attr
            for _, attr in self.__class__.__dict__.items()
            if isinstance(attr, Task) and attr.type == step_type
        ]

    @property
    def on_failure_steps(self) -> list[Task[Any]]:
        return self._get_steps_by_type(StepType.ON_FAILURE)

    @property
    def concurrency_actions(self) -> list[Task[Any]]:
        return self._get_steps_by_type(StepType.CONCURRENCY)

    @property
    def default_steps(self) -> list[Task[Any]]:
        return self._get_steps_by_type(StepType.DEFAULT)

    @property
    def steps(self) -> list[Task[Any]]:
        return self.default_steps + self.concurrency_actions + self.on_failure_steps

    def create_action_name(self, namespace: str, step: Task[Any]) -> str:
        return self.get_service_name(namespace) + ":" + step.name

    def get_name(self, namespace: str) -> str:
        return namespace + self.config.name

    def validate_concurrency_actions(
        self, service_name: str
    ) -> WorkflowConcurrencyOpts | None:
        if len(self.concurrency_actions) > 0 and self.config.concurrency:
            raise ValueError(
                "Error: Both concurrencyActions and concurrency_expression are defined. Please use only one concurrency configuration method."
            )

        if len(self.concurrency_actions) > 0:
            action = self.concurrency_actions[0]

            return WorkflowConcurrencyOpts(
                action=service_name + ":" + action.name,
                max_runs=action.concurrency__max_runs,
                limit_strategy=maybe_int_to_str(
                    convert_python_enum_to_proto(
                        action.concurrency__limit_strategy,
                        ConcurrencyLimitStrategyProto,
                    )
                ),
            )

        if self.config.concurrency:
            return WorkflowConcurrencyOpts(
                expression=self.config.concurrency.expression,
                max_runs=self.config.concurrency.max_runs,
                limit_strategy=self.config.concurrency.limit_strategy,
            )

        return None

    def validate_on_failure_steps(
        self, name: str, service_name: str
    ) -> CreateWorkflowJobOpts | None:
        if not self.on_failure_steps:
            return None

        on_failure_step = next(iter(self.on_failure_steps))

        return CreateWorkflowJobOpts(
            name=name + "-on-failure",
            steps=[
                CreateWorkflowStepOpts(
                    readable_id=on_failure_step.name,
                    action=service_name + ":" + on_failure_step.name,
                    timeout=on_failure_step.timeout or "60s",
                    inputs="{}",
                    parents=[],
                    retries=on_failure_step.retries,
                    rate_limits=on_failure_step.rate_limits,
                    backoff_factor=on_failure_step.backoff_factor,
                    backoff_max_seconds=on_failure_step.backoff_max_seconds,
                )
            ],
        )

    def validate_priority(self, default_priority: int | None) -> int | None:
        validated_priority = (
            max(1, min(3, default_priority)) if default_priority else None
        )
        if validated_priority != default_priority:
            logger.warning(
                "Warning: Default Priority Must be between 1 and 3 -- inclusively. Adjusted to be within the range."
            )

        return validated_priority

    def get_create_opts(self, namespace: str) -> CreateWorkflowVersionOpts:
        service_name = self.get_service_name(namespace)

        name = self.get_name(namespace)
        event_triggers = [namespace + event for event in self.config.on_events]

        create_step_opts = [
            CreateWorkflowStepOpts(
                readable_id=step.name,
                action=service_name + ":" + step.name,
                timeout=step.timeout or "60s",
                inputs="{}",
                parents=[x for x in step.parents],
                retries=step.retries,
                rate_limits=step.rate_limits,
                worker_labels=step.desired_worker_labels,
                backoff_factor=step.backoff_factor,
                backoff_max_seconds=step.backoff_max_seconds,
            )
            for step in self.steps
            if step.type == StepType.DEFAULT
        ]

        concurrency = self.validate_concurrency_actions(service_name)
        on_failure_job = self.validate_on_failure_steps(name, service_name)
        validated_priority = self.validate_priority(self.config.default_priority)

        return CreateWorkflowVersionOpts(
            name=name,
            kind=WorkflowKind.DAG,
            version=self.config.version,
            event_triggers=event_triggers,
            cron_triggers=self.config.on_crons,
            schedule_timeout=self.config.schedule_timeout,
            sticky=maybe_int_to_str(
                convert_python_enum_to_proto(self.config.sticky, StickyStrategyProto)
            ),
            jobs=[
                CreateWorkflowJobOpts(
                    name=name,
                    steps=create_step_opts,
                )
            ],
            on_failure_job=on_failure_job,
            concurrency=concurrency,
            default_priority=validated_priority,
        )
