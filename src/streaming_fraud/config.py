"""Environment-driven settings (SF_* variables) for the streaming pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str = "localhost:19092"
    transactions_topic: str = "transactions.raw"
    scores_topic: str = "transactions.scored"
    alerts_topic: str = "fraud.alerts"
    dlq_topic: str = "transactions.dlq"
    group_id: str = "fraud-scorer"
    model_path: str = "models/fraud_model.joblib"
    metrics_port: int = 8001


def load() -> Settings:
    """Build settings from SF_* environment variables with local-dev defaults."""
    env = os.environ.get
    return Settings(
        bootstrap_servers=env("SF_BOOTSTRAP_SERVERS", "localhost:19092"),
        transactions_topic=env("SF_TRANSACTIONS_TOPIC", "transactions.raw"),
        scores_topic=env("SF_SCORES_TOPIC", "transactions.scored"),
        alerts_topic=env("SF_ALERTS_TOPIC", "fraud.alerts"),
        dlq_topic=env("SF_DLQ_TOPIC", "transactions.dlq"),
        group_id=env("SF_GROUP_ID", "fraud-scorer"),
        model_path=env("SF_MODEL_PATH", "models/fraud_model.joblib"),
        metrics_port=int(env("SF_METRICS_PORT", "8001")),
    )
