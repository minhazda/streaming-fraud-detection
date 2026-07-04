"""Message contracts for the transaction stream.

The transaction payload mirrors the raw schema produced by
``fraud_detection.data.generate`` (minus the label, which never exists at
serve time). Validation is strict: unknown fields are rejected so schema
drift surfaces in the DLQ instead of silently reaching the model.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class Transaction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    transaction_id: str = Field(min_length=1)
    produced_at: float = Field(gt=0, description="Unix timestamp set by the producer")
    amount: float = Field(gt=0)
    hour: int = Field(ge=0, le=23)
    day_of_week: int = Field(ge=0, le=6)
    merchant_category: str = Field(min_length=1)
    customer_age: int = Field(ge=18, le=120)
    account_age_days: int = Field(ge=0)
    n_tx_24h: int = Field(ge=0)
    avg_amount_30d: float = Field(ge=0)
    distance_from_home: float = Field(ge=0)
    is_foreign: int = Field(ge=0, le=1)


class ScoreResult(BaseModel):
    transaction_id: str
    fraud_probability: float
    is_fraud: bool
    threshold: float
    scored_at: float
