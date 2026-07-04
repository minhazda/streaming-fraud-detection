"""Message-bus abstraction: a real Kafka client and an in-memory double.

The pipeline is written against ``MessageBus`` so unit tests exercise the full
consume -> validate -> score -> route path without a broker; the integration
suite swaps in ``KafkaBus`` against a live Redpanda.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class Message:
    topic: str
    key: str
    value: bytes


class MessageBus(Protocol):
    def produce(self, topic: str, key: str, value: bytes) -> None: ...

    def flush(self) -> None: ...

    def consume(self, topic: str, *, limit: int | None, idle_timeout: float) -> Iterator[Message]:
        """Yield messages from ``topic``.

        Stops after ``limit`` messages, or after ``idle_timeout`` seconds pass
        with nothing to read (``limit=None`` streams until idle).
        """
        ...


class InMemoryBus:
    """Deterministic bus for unit tests: topics are plain lists."""

    def __init__(self) -> None:
        self.topics: dict[str, list[Message]] = defaultdict(list)

    def produce(self, topic: str, key: str, value: bytes) -> None:
        self.topics[topic].append(Message(topic, key, value))

    def flush(self) -> None:  # pragma: no cover - nothing buffered
        return None

    def consume(self, topic: str, *, limit: int | None, idle_timeout: float) -> Iterator[Message]:
        del idle_timeout  # in-memory reads never block
        messages = self.topics[topic]
        n = len(messages) if limit is None else min(limit, len(messages))
        yield from messages[:n]


class KafkaBus:
    """Thin confluent-kafka wrapper. Imported lazily so unit tests never need it."""

    def __init__(self, bootstrap_servers: str, group_id: str) -> None:
        from confluent_kafka import Consumer, Producer

        self._producer = Producer({"bootstrap.servers": bootstrap_servers})
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )

    def produce(self, topic: str, key: str, value: bytes) -> None:
        self._producer.produce(topic, key=key.encode(), value=value)
        self._producer.poll(0)

    def flush(self) -> None:
        # flush() returns the number of messages STILL undelivered. Ignoring it
        # means a producer can exit 0 having delivered nothing (e.g. broker not
        # up yet) - a silent-loss bug we hit in the k8s smoke test.
        remaining = self._producer.flush(30)
        if remaining:
            raise RuntimeError(f"{remaining} messages not delivered before flush timeout")

    def consume(self, topic: str, *, limit: int | None, idle_timeout: float) -> Iterator[Message]:
        self._consumer.subscribe([topic])
        seen = 0
        idle = 0.0
        poll_s = 0.5
        while limit is None or seen < limit:
            record = self._consumer.poll(poll_s)
            if record is None:
                idle += poll_s
                if idle >= idle_timeout:
                    break
                continue
            if record.error():  # pragma: no cover - broker-side condition
                continue
            idle = 0.0
            seen += 1
            key = record.key()
            yield Message(
                topic=record.topic() or topic,
                key=key.decode() if key else "",
                value=record.value() or b"",
            )

    def close(self) -> None:
        self._consumer.close()
