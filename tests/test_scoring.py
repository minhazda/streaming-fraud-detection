"""Scorer behaviour against the real (small) trained bundle."""

from __future__ import annotations

from streaming_fraud.schema import Transaction
from streaming_fraud.scoring import Scorer
from tests.conftest import make_txn_payload
from tests.test_schema import VALID


def test_probability_in_unit_interval(scorer: Scorer) -> None:
    result = scorer.score(Transaction.model_validate(VALID))
    assert 0.0 <= result.fraud_probability <= 1.0
    assert result.threshold == scorer.threshold


def test_unseen_merchant_category_degrades_gracefully(scorer: Scorer) -> None:
    """A category the model never saw must score, not crash (reindex fill 0)."""
    txn = Transaction.model_validate({**VALID, "merchant_category": "quantum_pets"})
    result = scorer.score(txn)
    assert 0.0 <= result.fraud_probability <= 1.0


def test_risky_profile_scores_higher_than_safe_profile(scorer: Scorer) -> None:
    """Directional sanity: the known risk signals push the score up."""
    safe = Transaction.model_validate(
        {**VALID, "hour": 12, "merchant_category": "grocery", "is_foreign": 0, "amount": 20.0}
    )
    risky = Transaction.model_validate(
        {
            **VALID,
            "hour": 3,
            "merchant_category": "gambling",
            "is_foreign": 1,
            "amount": 900.0,
            "account_age_days": 5,
            "distance_from_home": 400.0,
        }
    )
    assert scorer.score(risky).fraud_probability > scorer.score(safe).fraud_probability


def test_scores_match_batch_predictions(scorer: Scorer, sample_rows: list[dict]) -> None:
    """Stream scoring one-by-one equals what the model says: no serve-time skew."""
    import pandas as pd
    from fraud_detection.features import build_features

    payloads = [make_txn_payload(r) for r in sample_rows[:20]]
    raw = pd.DataFrame(
        [
            {k: v for k, v in p.items() if k not in ("transaction_id", "produced_at")}
            for p in payloads
        ]
    )
    batch = build_features(raw).reindex(columns=scorer.feature_names, fill_value=0)
    expected = scorer._model.predict_proba(batch.astype(float))[:, 1]

    for payload, want in zip(payloads, expected, strict=True):
        got = scorer.score(Transaction.model_validate(payload)).fraud_probability
        assert abs(got - float(want)) < 1e-9
