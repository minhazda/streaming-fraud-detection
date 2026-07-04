"""Shared fixtures: a small real model bundle trained once per session."""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

import pytest
from fraud_detection.data.generate import generate

from streaming_fraud.config import Settings
from streaming_fraud.scoring import Scorer


@pytest.fixture(scope="session")
def model_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Train the real fraud model on a small sample; fast (<10s) but genuine."""
    from fraud_detection import train

    root = tmp_path_factory.mktemp("model")
    config = root / "config.yaml"
    config.write_text(json.dumps({"n_samples": 6000, "seed": 7, "test_size": 0.4}))
    bundle = root / "fraud_model.joblib"

    import os

    os.environ["FD_CONFIG"] = str(config)
    os.environ["FD_MODEL_PATH"] = str(bundle)
    try:
        train.main()
    finally:
        os.environ.pop("FD_CONFIG", None)
        os.environ.pop("FD_MODEL_PATH", None)
    return bundle


@pytest.fixture(scope="session")
def scorer(model_path: Path) -> Scorer:
    return Scorer(model_path)


@pytest.fixture()
def settings() -> Settings:
    return Settings()


def make_txn_payload(row: dict[str, object]) -> dict[str, object]:
    """Raw generator row -> valid wire payload (adds id + timestamp, drops label)."""
    payload = {k: (v.item() if hasattr(v, "item") else v) for k, v in row.items()}
    payload.pop("is_fraud", None)
    payload["transaction_id"] = str(uuid.uuid4())
    payload["produced_at"] = time.time()
    return payload


@pytest.fixture()
def sample_rows() -> list[dict[str, object]]:
    return generate(60, seed=11).to_dict(orient="records")
