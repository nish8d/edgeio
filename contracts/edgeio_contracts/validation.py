"""Single entry point consumers use to accept or reject a raw message."""

from datetime import datetime, timedelta
from typing import Literal

from pydantic import ValidationError

from .models import HealthReport

ErrorStage = Literal["decode", "schema", "semantic"]
MAX_CLOCK_SKEW = timedelta(minutes=10)


class ContractError(Exception):
    """A payload that violates the device.health contract."""

    def __init__(self, stage: ErrorStage, message: str) -> None:
        super().__init__(message)
        self.stage: ErrorStage = stage
        self.message = message


def validate_report(raw: bytes | str | None, now: datetime) -> HealthReport:
    if raw is None:
        raise ContractError("decode", "message has no value")
    try:
        report = HealthReport.model_validate_json(raw)
    except ValidationError as exc:
        stage: ErrorStage = (
            "decode" if any(err["type"] == "json_invalid" for err in exc.errors()) else "schema"
        )
        raise ContractError(stage, _summarize(exc)) from exc
    if report.timestamp > now + MAX_CLOCK_SKEW:
        raise ContractError(
            "semantic",
            f"timestamp {report.timestamp.isoformat()} is more than 10 minutes in the future",
        )
    return report


def _summarize(exc: ValidationError, limit: int = 5) -> str:
    parts = []
    for err in exc.errors()[:limit]:
        location = ".".join(str(part) for part in err["loc"]) or "<root>"
        parts.append(f"{location}: {err['msg']}")
    return "; ".join(parts)
