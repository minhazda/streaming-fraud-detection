"""End-to-end pipeline behaviour on the in-memory bus (no broker required)."""

from __future__ import annotations

import json

from streaming_fraud.broker import InMemoryBus
from streaming_fraud.config import Settings
from streaming_fraud.pipeline import run
from streaming_fraud.schema import ScoreResult
from streaming_fraud.scoring import Scorer
from tests.conftest import make_txn_payload


def _seed_bus(bus: InMemoryBus, settings: Settings, rows: list[dict], invalid: int) -> int:
    for row in rows:
        payload = make_txn_payload(row)
        bus.produce(
            settings.transactions_topic, payload["transaction_id"], json.dumps(payload).encode()
        )
    for i in range(invalid):
        bus.produce(settings.transactions_topic, f"bad-{i}", b"{definitely not json")
    return len(rows) + invalid


def test_routes_scores_alerts_and_dlq(
    scorer: Scorer, settings: Settings, sample_rows: list[dict]
) -> None:
    bus = InMemoryBus()
    total = _seed_bus(bus, settings, sample_rows, invalid=3)

    counts = run(bus, scorer, settings, max_messages=total)

    assert counts["scored"] == len(sample_rows)
    assert counts["invalid"] == 3
    assert len(bus.topics[settings.scores_topic]) == len(sample_rows)
    assert len(bus.topics[settings.dlq_topic]) == 3
    assert len(bus.topics[settings.alerts_topic]) == counts["alerts"]


def test_alerts_are_exactly_the_above_threshold_scores(
    scorer: Scorer, settings: Settings, sample_rows: list[dict]
) -> None:
    bus = InMemoryBus()
    total = _seed_bus(bus, settings, sample_rows, invalid=0)
    run(bus, scorer, settings, max_messages=total)

    results = [ScoreResult.model_validate_json(m.value) for m in bus.topics[settings.scores_topic]]
    flagged = {r.transaction_id for r in results if r.fraud_probability >= scorer.threshold}
    alerted = {
        ScoreResult.model_validate_json(m.value).transaction_id
        for m in bus.topics[settings.alerts_topic]
    }
    assert flagged == alerted


def test_dlq_payload_carries_reason_and_raw(scorer: Scorer, settings: Settings) -> None:
    bus = InMemoryBus()
    bus.produce(settings.transactions_topic, "k", json.dumps({"amount": -5}).encode())
    run(bus, scorer, settings, max_messages=1)

    (dead,) = bus.topics[settings.dlq_topic]
    body = json.loads(dead.value)
    assert "reason" in body and "raw" in body
