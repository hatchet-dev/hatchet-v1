from typing import Any, cast

from pydantic import BaseModel

from hatchet_sdk import (
    BaseWorkflow,
    ChildTriggerWorkflowOptions,
    Context,
    Hatchet,
    SpawnWorkflowInput,
)

hatchet = Hatchet(debug=True)


class ParentInput(BaseModel):
    n: int = 5


class ChildInput(BaseModel):
    a: str


parent = hatchet.declare_workflow(
    on_events=["parent:create"], input_validator=ParentInput
)
child = hatchet.declare_workflow(on_events=["child:create"], input_validator=ChildInput)


class SyncFanoutParent(BaseWorkflow):
    @parent.task(timeout="5m")
    def spawn(self, context: Context) -> dict[str, Any]:
        print("spawning child")

        n = parent.get_workflow_input(context).n

        runs = child.spawn_many(
            context,
            [
                SpawnWorkflowInput(
                    input=ChildInput(a=str(i)),
                    key=f"child{i}",
                    options=ChildTriggerWorkflowOptions(
                        additional_metadata={"hello": "earth"}
                    ),
                )
                for i in range(n)
            ],
        )

        results = [r.result() for r in runs]

        print(f"results {results}")

        return {"results": results}


class SyncFanoutChild(BaseWorkflow):
    @child.task()
    def process(self, context: Context) -> dict[str, str]:
        a = cast(str, context.workflow_input["a"])
        return {"status": "success " + a}


def main() -> None:
    worker = hatchet.worker(
        "sync-fanout-worker",
        max_runs=40,
        workflows=[
            SyncFanoutParent(parent),
            SyncFanoutChild(child),
        ],
    )
    worker.start()


if __name__ == "__main__":
    main()
