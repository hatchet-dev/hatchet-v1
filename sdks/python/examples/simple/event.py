from hatchet_sdk import Hatchet

hatchet = Hatchet()

hatchet.admin.run_workflow(
    "SimpleTask",
    {"test": "test"},
)
