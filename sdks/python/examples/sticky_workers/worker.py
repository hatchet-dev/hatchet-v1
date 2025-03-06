from hatchet_sdk import (
    BaseWorkflow,
    ChildTriggerWorkflowOptions,
    Context,
    Hatchet,
    StickyStrategy,
)

hatchet = Hatchet(debug=True)

sticky_workflow = hatchet.declare_workflow(
    on_events=["sticky:parent"], sticky=StickyStrategy.SOFT
)


class StickyWorkflow(BaseWorkflow):
    @sticky_workflow.task()
    def step1a(self, context: Context) -> dict[str, str | None]:
        return {"worker": context.worker.id()}

    @sticky_workflow.task()
    def step1b(self, context: Context) -> dict[str, str | None]:
        return {"worker": context.worker.id()}

    @sticky_workflow.task(parents=["step1a", "step1b"])
    async def step2(self, context: Context) -> dict[str, str | None]:
        ref = await context.aio_spawn_workflow(
            "StickyChildWorkflow", {}, options=ChildTriggerWorkflowOptions(sticky=True)
        )

        await ref.aio_result()

        return {"worker": context.worker.id()}


sticky_child_workflow = hatchet.declare_workflow(
    on_events=["sticky:child"], sticky=StickyStrategy.SOFT
)


class StickyChildWorkflow(BaseWorkflow):
    @sticky_child_workflow.task()
    def child(self, context: Context) -> dict[str, str | None]:
        return {"worker": context.worker.id()}


def main() -> None:
    worker = hatchet.worker("sticky-worker", max_runs=10)
    worker.register_workflow(StickyWorkflow(sticky_workflow))
    worker.register_workflow(StickyChildWorkflow(sticky_child_workflow))
    worker.start()


if __name__ == "__main__":
    main()
