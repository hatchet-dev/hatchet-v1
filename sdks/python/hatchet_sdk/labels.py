from pydantic import BaseModel

from hatchet_sdk.contracts.workflows_pb2 import DesiredWorkerLabels


class DesiredWorkerLabel(BaseModel):
    value: str | int
    required: bool = False
    weight: int | None = None
    comparator: int | None = None


def transform_desired_worker_label(d: DesiredWorkerLabel) -> DesiredWorkerLabels:
    value = d.value
    return DesiredWorkerLabels(
        strValue=value if not isinstance(value, int) else None,
        intValue=value if isinstance(value, int) else None,
        required=d.required,
        weight=d.weight,
        comparator=d.comparator,  # type: ignore[arg-type]
    )
