import random

from edgeio_simulator.faults import ActiveFault, Fault, next_fault, start_fault
from edgeio_simulator.fleet import SERVICES, make_profile

PROFILE = make_profile(0, seed=7)


def test_no_fault_starts_when_rate_is_zero() -> None:
    rng = random.Random(1)
    assert all(next_fault(None, rng, 0.0, PROFILE) is None for _ in range(1000))


def test_fault_starts_when_rate_is_one() -> None:
    fault = next_fault(None, random.Random(1), 1.0, PROFILE)
    assert fault is not None
    assert fault.ticks_left >= 1


def test_fault_counts_down_then_ends() -> None:
    fault: ActiveFault | None = ActiveFault(Fault.OVERHEAT, ticks_left=3)
    rng = random.Random(1)
    seen = []
    for _ in range(3):
        fault = next_fault(fault, rng, 1.0, PROFILE)
        seen.append(fault.ticks_left if fault else None)
    assert seen == [2, 1, None]


def test_reboot_lasts_one_tick() -> None:
    assert start_fault(Fault.REBOOT, random.Random(1), PROFILE).ticks_left == 1


def test_service_crash_targets_a_known_service() -> None:
    for seed in range(20):
        fault = start_fault(Fault.SERVICE_CRASH, random.Random(seed), PROFILE)
        assert fault.target in SERVICES


def test_container_crash_magnitude_fits_the_device() -> None:
    for seed in range(20):
        fault = start_fault(Fault.CONTAINER_CRASH, random.Random(seed), PROFILE)
        assert 1 <= fault.magnitude <= min(2, PROFILE.container_count)


def test_offline_can_outlast_the_offline_alert_window() -> None:
    durations = {
        start_fault(Fault.OFFLINE, random.Random(s), PROFILE).ticks_left for s in range(200)
    }
    assert max(durations) >= 4  # > 15 min at 300 s per tick
