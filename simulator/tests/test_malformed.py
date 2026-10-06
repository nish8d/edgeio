from datetime import UTC, datetime

import pytest

from edgeio_contracts.samples import sample_report
from edgeio_contracts.validation import ContractError, validate_report
from edgeio_simulator.malformed import MALFORMATIONS, malform


@pytest.mark.parametrize("kind", MALFORMATIONS)
def test_every_malformation_is_rejected_by_the_contract(kind: str) -> None:
    raw = malform(sample_report(), kind)
    with pytest.raises(ContractError):
        validate_report(raw, now=datetime(2026, 10, 6, 10, 0, tzinfo=UTC))


def test_unknown_malformation_is_an_error() -> None:
    with pytest.raises(ValueError):
        malform(sample_report(), "nope")
