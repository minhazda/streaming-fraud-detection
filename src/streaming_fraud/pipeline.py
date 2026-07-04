"""The stream processor: consume -> validate -> score -> route.

Routing:
- every valid transaction   -> scores topic
- score above threshold     -> alerts topic (in addition to scores)
- invalid / malformed input -> dead-letter queue with the rejection reason

Prometheus metrics are exposed on ``/metrics`` (port ``SF_METRICS_PORT``).
"""

from __future__ import annotations

import json
import logging
import time

from prometheus_client import Counter, Histogram, start_http_server
from pydantic import ValidationError

from .broker import KafkaBus, MessageBus
from .config import Settings, load
from .schema import Transaction
from .scoring import Scorer

log = logging.getLogger("streaming_fraud.pipeline")

TRANSACTIONS = Counter("sf_transactions_total", "Transactions consumed, by outcome", ["outcome"])
ALERTS = Counter("sf_alerts_total", "Transactions flagged as fraud")
SCORING_SECONDS = Histogram(
    "sf_scoring_seconds",
    "Model scoring time per transaction",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25),
)
E2E_SECONDS = Histogram(
    "sf_end_to_end_seconds",
    "Producer-to-scored latency per transaction",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)


def _dead_letter(bus: MessageBus, settings: Settings, message_value: bytes, reason: str) -> None:
    TRANSACTIONS.labels(outcome="invalid").inc()
    payload = json.dumps(
        {"reason": reason, "raw": message_value.decode(errors="replace"), "at": time.time()}
    )
    bus.produce(settings.dlq_topic, key="dlq", value=payload.encode())


def run(
    bus: MessageBus,
    scorer: Scorer,
    settings: Settings,
    *,
    max_messages: int | None = None,
    idle_timeout: float = 30.0,
) -> dict[str, int]:
    """Process the transactions topic; return counts for observability/tests."""
    counts = {"scored": 0, "alerts": 0, "invalid": 0}
    for message in bus.consume(
        settings.transactions_topic, limit=max_messages, idle_timeout=idle_timeout
    ):
        try:
            txn = Transaction.model_validate_json(message.value)
        except ValidationError as exc:
            _dead_letter(bus, settings, message.value, reason=str(exc.errors()[:3]))
            counts["invalid"] += 1
            continue

        started = time.perf_counter()
        result = scorer.score(txn)
        SCORING_SECONDS.observe(time.perf_counter() - started)
        E2E_SECONDS.observe(max(result.scored_at - txn.produced_at, 0.0))
        TRANSACTIONS.labels(outcome="scored").inc()
        counts["scored"] += 1

        body = result.model_dump_json().encode()
        bus.produce(settings.scores_topic, key=txn.transaction_id, value=body)
        if result.is_fraud:
            ALERTS.inc()
            counts["alerts"] += 1
            bus.produce(settings.alerts_topic, key=txn.transaction_id, value=body)

    bus.flush()
    return counts


def main() -> None:  # pragma: no cover - thin runtime wrapper
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    settings = load()
    start_http_server(settings.metrics_port)
    scorer = Scorer(settings.model_path)
    bus = KafkaBus(settings.bootstrap_servers, settings.group_id)
    log.info(
        "pipeline up: broker=%s topic=%s threshold=%.3f metrics=:%d",
        settings.bootstrap_servers,
        settings.transactions_topic,
        scorer.threshold,
        settings.metrics_port,
    )
    try:
        counts = run(bus, scorer, settings, idle_timeout=float("inf"))
    finally:
        bus.close()
    log.info("pipeline stopped: %s", counts)


if __name__ == "__main__":
    main()
