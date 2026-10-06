from confluent_kafka import OFFSET_INVALID, TopicPartition

from edgeio_worker.lag import consumer_lag


class FakeConsumer:
    """Mimics the parts of confluent_kafka.Consumer that consumer_lag uses."""

    def __init__(
        self,
        watermarks: dict[int, tuple[int, int]],
        positions: dict[int, int] | None = None,
        committed: dict[int, int] | None = None,
    ) -> None:
        self._watermarks = watermarks
        self._positions = positions or {}
        self._committed = committed or {}

    def assignment(self) -> list[TopicPartition]:
        return [TopicPartition("t", p) for p in self._watermarks]

    def position(self, parts: list[TopicPartition]) -> list[TopicPartition]:
        return [
            TopicPartition(
                tp.topic, tp.partition, self._positions.get(tp.partition, OFFSET_INVALID)
            )
            for tp in parts
        ]

    def committed(self, parts: list[TopicPartition], timeout: float = -1) -> list[TopicPartition]:
        return [
            TopicPartition(
                tp.topic, tp.partition, self._committed.get(tp.partition, OFFSET_INVALID)
            )
            for tp in parts
        ]

    def get_watermark_offsets(self, tp: TopicPartition, timeout: float = -1) -> tuple[int, int]:
        return self._watermarks[tp.partition]


def test_unassigned_consumer_has_unknown_lag() -> None:
    assert consumer_lag(FakeConsumer({})) is None


def test_lag_is_high_watermark_minus_position() -> None:
    consumer = FakeConsumer({0: (0, 10), 1: (0, 5)}, positions={0: 7, 1: 5})
    assert consumer_lag(consumer) == 3


def test_committed_offset_is_used_before_first_message() -> None:
    assert consumer_lag(FakeConsumer({0: (0, 10)}, committed={0: 10})) == 0


def test_falls_back_to_low_watermark_without_any_offset() -> None:
    assert consumer_lag(FakeConsumer({0: (2, 5)})) == 3
