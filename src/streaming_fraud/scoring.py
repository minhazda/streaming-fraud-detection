"""Model loading and single-transaction scoring.

Feature engineering is imported from ``fraud_detection.features`` — the exact
module the model was trained with — so the streaming path cannot drift from
the training path.
"""

from __future__ import annotations

import time
from pathlib import Path

import joblib
import pandas as pd
from fraud_detection.features import build_features

from .schema import ScoreResult, Transaction


class Scorer:
    """Scores raw transactions with the persisted {model, features, threshold} bundle."""

    def __init__(self, bundle_path: str | Path) -> None:
        bundle = joblib.load(bundle_path)
        self._model = bundle["model"]
        self.feature_names: list[str] = list(bundle["features"])
        self.threshold: float = float(bundle["threshold"])

    def score(self, txn: Transaction) -> ScoreResult:
        raw = pd.DataFrame([txn.model_dump(exclude={"transaction_id", "produced_at"})])
        # Align to the training-time columns: unseen one-hot categories become 0,
        # so a brand-new merchant_category degrades gracefully instead of crashing.
        features = build_features(raw).reindex(columns=self.feature_names, fill_value=0)
        probability = float(self._model.predict_proba(features.astype(float))[0, 1])
        return ScoreResult(
            transaction_id=txn.transaction_id,
            fraud_probability=probability,
            is_fraud=probability >= self.threshold,
            threshold=self.threshold,
            scored_at=time.time(),
        )
