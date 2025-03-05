import asyncio

from hatchet_sdk import Hatchet

hatchet = Hatchet()


async def main() -> None:
    hatchet.admin.run_workflow(
        "Parent",
        {"x": "foo bar baz"},
    )


if __name__ == "__main__":
    asyncio.run(main())
