import pytest

from edgeio_agent.ping import internet_packet_loss, parse_loss

OK = "5 packets transmitted, 5 received, 0% packet loss, time 804ms\n"
LOSSY = "5 packets transmitted, 3 received, 40% packet loss, time 812ms\n"
DROPPED = "5 packets transmitted, 0 received, 100% packet loss, time 4005ms\n"
UNREACHABLE = "ping: connect: Network is unreachable\n"


def test_parse_loss() -> None:
    assert parse_loss(OK) == 0.0
    assert parse_loss(LOSSY) == 40.0
    assert parse_loss(DROPPED) == 100.0
    assert parse_loss("5 packets transmitted, 0 received, +5 errors, 100% packet loss") == 100.0


def test_unparseable_output_counts_as_total_loss() -> None:
    assert parse_loss(UNREACHABLE) == 100.0
    assert parse_loss("") == 100.0


def test_loss_is_the_best_target_so_one_provider_dropping_icmp_is_not_an_outage() -> None:
    outputs = {"1.1.1.1": DROPPED, "8.8.8.8": LOSSY}

    def runner(target: str, count: int) -> str:
        return outputs[target]

    assert internet_packet_loss(["1.1.1.1", "8.8.8.8"], 5, runner) == 40.0


def test_loss_is_100_when_every_target_fails() -> None:
    def runner(target: str, count: int) -> str:
        return UNREACHABLE

    assert internet_packet_loss(["1.1.1.1", "8.8.8.8"], 5, runner) == 100.0


def test_runner_receives_target_and_count() -> None:
    seen: list[tuple[str, int]] = []

    def runner(target: str, count: int) -> str:
        seen.append((target, count))
        return OK

    internet_packet_loss(["9.9.9.9"], 3, runner)
    assert seen == [("9.9.9.9", 3)]


def test_no_targets_is_a_configuration_error() -> None:
    with pytest.raises(ValueError, match="ping target"):
        internet_packet_loss([], 5)
