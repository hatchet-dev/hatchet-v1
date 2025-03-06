from hatchet_sdk import Context, Hatchet

hatchet = Hatchet(debug=True)


@hatchet.task(name="SimpleTask")
async def step1(context: Context) -> dict[str, str]:
    print("executed step1")
    return {
        "step1": "step1",
    }


def main() -> None:
    worker = hatchet.worker("test-worker", max_runs=1)
    worker.register_workflow(step1)
    worker.start()


if __name__ == "__main__":
    main()
