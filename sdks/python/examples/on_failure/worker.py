import json

from hatchet_sdk import BaseWorkflow, Context, Hatchet

hatchet = Hatchet(debug=True)

# ❓ OnFailure Step
# This workflow will fail because the step will throw an error
# we define an onFailure step to handle this case

on_failure_wf = hatchet.declare_workflow(on_events=["user:create"])


class OnFailureWorkflow(BaseWorkflow):
    @on_failure_wf.task(timeout="1s")
    def step1(self, context: Context) -> None:
        # 👀 this step will always raise an exception
        raise Exception("step1 failed")

    # 👀 After the workflow fails, this special step will run
    @on_failure_wf.on_failure_task()
    def on_failure(self, context: Context) -> dict[str, str]:
        # 👀 we can do things like perform cleanup logic
        # or notify a user here

        # 👀 Fetch the errors from upstream step runs from the context
        print(context.step_run_errors)

        return {"status": "success"}


# ‼️


# ❓ OnFailure With Details
# We can access the failure details in the onFailure step
# via the context method

on_failure_wf_with_details = hatchet.declare_workflow(on_events=["user:create"])


class OnFailureWorkflowWithDetails(BaseWorkflow):
    config = on_failure_wf_with_details.config

    # ... defined as above
    @on_failure_wf_with_details.task(timeout="1s")
    def step1(self, context: Context) -> None:
        raise Exception("step1 failed")

    # 👀 After the workflow fails, this special step will run
    @on_failure_wf_with_details.on_failure_task()
    def on_failure(self, context: Context) -> dict[str, str]:
        failures = context.fetch_run_failures()

        # 👀 we can access the failure details here
        print(json.dumps(failures, indent=2))
        if len(failures) == 1 and "step1 failed" in failures[0].error:
            return {"status": "success"}

        raise Exception("unexpected failure")


# ‼️


def main() -> None:
    worker = hatchet.worker(
        "on-failure-worker",
        max_runs=4,
        workflows=[
            OnFailureWorkflow(on_failure_wf),
            OnFailureWorkflowWithDetails(on_failure_wf_with_details),
        ],
    )

    worker.start()


if __name__ == "__main__":
    main()
