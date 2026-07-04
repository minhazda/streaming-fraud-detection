"""End-to-end test against a live Redpanda broker (docker compose up -d redpanda).

Run explicitly with:  pytest -m integration
Skipped automatically when no broker answers on SF_BOOTSTRAP_SERVERS.
"""

from __future__ import annotations

import json
import time
import uuid

import pytest

from streaming_fraud.broker import KafkaBus
from streaming_fraud.config import load
from streaming_fraud.pipeline import run
from streaming_fraud.producer import replay
from streaming_fraud.schema import ScoreResult
from streaming_fraud.scoring import Scorer

pytestmark = pytest.mark.integration

N_TRANSACTIONS = 300
INVALID_EVERY = 50  # -> 6 dead-lettered messages


def _broker_reachable(bootstrap: str) -> bool:
    try:
        from confluent_kafka.admin import AdminClient

        AdminClient({"bootstrap.servers": bootstrap, "socket.timeout.ms": 3000}).list_topics(
            timeout=5
        )
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def settings():
    # Unique topics per run so retries on the same broker start clean.
    run_id = uuid.uuid4().hex[:8]
    base = load()
    cfg = base.__class__(
        bootstrap_servers=base.bootstrap_servers,
        transactions_topic=f"it.{run_id}.transactions",
        scores_topic=f"it.{run_id}.scores",
        alerts_topic=f"it.{run_id}.alerts",
        dlq_topic=f"it.{run_id}.dlq",
        group_id=f"it-{run_id}",
        model_path=base.model_path,
        metrics_port=base.metrics_port,
    )
    if not _broker_reachable(cfg.bootstrap_servers):
        pytest.skip(f"no broker at {cfg.bootstrap_servers}")
    return cfg


def test_full_stream_roundtrip(settings, scorer: Scorer) -> None:
    producer_bus = KafkaBus(settings.bootstrap_servers, group_id="it-producer")
    sent = replay(
        producer_bus, settings, n=N_TRANSACTIONS, rate=0, seed=13, invalid_every=INVALID_EVERY
    )
    assert sent == N_TRANSACTIONS

    pipeline_bus = KafkaBus(settings.bootstrap_servers, settings.group_id)
    started = time.perf_counter()
    counts = run(
        pipeline_bus,
        scorer,
        settings,
        max_messages=N_TRANSACTIONS + N_TRANSACTIONS // INVALID_EVERY,
        idle_timeout=30.0,
    )
    elapsed = time.perf_counter() - started

    assert counts["scored"] == N_TRANSACTIONS
    assert counts["invalid"] == N_TRANSACTIONS // INVALID_EVERY
    throughput = counts["scored"] / elapsed
    print(
        f"\nintegration: scored {counts['scored']} msgs in {elapsed:.2f}s "
        f"({throughput:.0f} msg/s), alerts={counts['alerts']}"
    )

    # Scores round-trip through the broker intact.
    reader = KafkaBus(settings.bootstrap_servers, group_id="it-verify")
    results = [
        ScoreResult.model_validate_json(m.value)
        for m in reader.consume(settings.scores_topic, limit=N_TRANSACTIONS, idle_timeout=30.0)
    ]
    assert len(results) == N_TRANSACTIONS
    assert all(0.0 <= r.fraud_probability <= 1.0 for r in results)

    # DLQ received exactly the malformed payloads.
    dlq = list(reader.consume(settings.dlq_topic, limit=None, idle_timeout=10.0))
    assert len(dlq) == N_TRANSACTIONS // INVALID_EVERY
    assert all("reason" in json.loads(m.value) for m in dlq)

    for bus in (producer_bus, pipeline_bus, reader):
        bus.close()
