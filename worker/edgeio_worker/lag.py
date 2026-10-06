"""How far the consumer is behind the end of its assigned partitions."""

from typing import Any

from confluent_kafka import OFFSET_INVALID


def consumer_lag(consumer: Any, timeout: float = 5.0) -> int | None:
    """Messages not yet consumed across the assignment; None while unassigned."""
    assignment = consumer.assignment()
    if not assignment:
        return None
    positions = consumer.position(assignment)
    committed = {tp.partition: tp.offset for tp in consumer.committed(assignment, timeout=timeout)}
    lag = 0
    for tp in positions:
        low, high = consumer.get_watermark_offsets(tp, timeout=timeout)
        offset = tp.offset
        if offset == OFFSET_INVALID:  # nothing consumed yet in this session
            offset = committed.get(tp.partition, OFFSET_INVALID)
        if offset == OFFSET_INVALID:  # nothing ever committed
            offset = low
        lag += max(0, high - offset)
    return lag
