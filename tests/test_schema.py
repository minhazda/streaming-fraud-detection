"""Contract tests for the wire schema."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from streaming_fraud.schema import Transaction

VALID = {
    "transaction_id": "t-1",
    "produced_at": 1_700_000_000.0,
    "amount": 42.5,
    "hour": 13,
    "day_of_week": 2,
    "merchant_category": "grocery",
    "customer_age": 30,
    "account_age_days": 400,
    "n_tx_24h": 2,
    "avg_amount_30d": 38.0,
    "distance_from_home": 3.2,
    "is_foreign": 0,
}


def test_valid_payload_parses() -> None:
    txn = Transaction.model_validate(VALID)
    assert txn.amount == 42.5


def test_out_of_range_hour_rejected() -> None:
    with pytest.raises(ValidationError):
        Transaction.model_validate({**VALID, "hour": 24})


def test_unknown_field_rejected() -> None:
    """extra=forbid: schema drift must surface, not silently pass through."""
    with pytest.raises(ValidationError):
        Transaction.model_validate({**VALID, "surprise": 1})


def test_negative_amount_rejected() -> None:
    with pytest.raises(ValidationError):
        Transaction.model_validate({**VALID, "amount": -1.0})
