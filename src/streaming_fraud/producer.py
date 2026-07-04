"""Replay producer: publishes synthetic raw transactions onto the stream.

Reuses the deterministic generator from fraud-detection-mlops, drops the label
(it does not exist at serve time), and optionally injects malformed payloads to
demonstrate dead-letter routing.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
import uuid

from fraud_detection.data.generate import generate

from .broker import KafkaBus, MessageBus
from .config import Settings, load

log = logging.getLogger("streaming_fraud.producer")


def replay(
    bus: MessageBus,
    settings: Settings,
    *,
    n: int = 1000,
    rate: float = 200.0,
    seed: int = 7,
    invalid_every: int = 0,
) -> int:
    """Publish ``n`` transactions at ``rate`` msg/s; returns the count sent."""
    df = generate(n, seed=seed).drop(columns=["is_fraud"])
    delay = 1.0 / rate if rate > 0 else 0.0
    sent = 0
    for i, row in enumerate(df.to_dict(orient="records")):
        if invalid_every and i % invalid_every == invalid_every - 1:
            bus.produce(settings.transactions_topic, key="bad", value=b"{not json")
        txn_id = str(uuid.uuid4())
        payload = {
            "transaction_id": txn_id,
            "produced_at": time.time(),
            **{k: (v.item() if hasattr(v, "item") else v) for k, v in row.items()},
        }
        bus.produce(settings.transactions_topic, key=txn_id, value=json.dumps(payload).encode())
        sent += 1
        if delay:
            time.sleep(delay)
    bus.flush()
    return sent


def main() -> None:  # pragma: no cover - thin runtime wrapper
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n", type=int, default=1000)
    parser.add_argument("--rate", type=float, default=200.0, help="messages per second")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument(
        "--invalid-every",
        type=int,
        default=0,
        help="inject one malformed message every N transactions (0 = never)",
    )
    args = parser.parse_args()

    settings = load()
    bus = KafkaBus(settings.bootstrap_servers, group_id="producer")
    sent = replay(
        bus,
        settings,
        n=args.n,
        rate=args.rate,
        seed=args.seed,
        invalid_every=args.invalid_every,
    )
    log.info("published %d transactions to %s", sent, settings.transactions_topic)


if __name__ == "__main__":
    main()
