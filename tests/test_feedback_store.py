from datetime import datetime, timedelta, timezone

import pytest

from feedback_store import (
    DecisionEvent,
    LabelEvent,
    append_decisions,
    append_labels,
    connect,
    point_in_time_training_rows,
    population_stability_index,
)


UTC = timezone.utc
T0 = datetime(2026, 1, 1, tzinfo=UTC)


def decision(decision_id: str = "dec-1") -> DecisionEvent:
    return DecisionEvent(
        decision_id=decision_id,
        event_ts=T0,
        entity_ref="customer-token",
        amount=125.0,
        risk_score=82.0,
        action="HARD_BLOCK",
        model_version="model-1",
        policy_version="policy-1",
        features={"velocity_1h": 11.0, "device_trust": 0.2},
    )


def test_decision_append_is_idempotent() -> None:
    db = connect()
    assert append_decisions(db, [decision()]) == 1
    assert append_decisions(db, [decision()]) == 0


def test_conflicting_replay_is_rejected() -> None:
    db = connect()
    append_decisions(db, [decision()])
    changed = DecisionEvent(**{**decision().__dict__, "risk_score": 81.0})
    with pytest.raises(ValueError, match="conflicting replay"):
        append_decisions(db, [changed])


def test_point_in_time_join_excludes_future_label() -> None:
    db = connect()
    append_decisions(db, [decision()])
    append_labels(
        db,
        [LabelEvent("lab-1", "dec-1", T0 + timedelta(days=45), True)],
    )
    assert point_in_time_training_rows(db, T0 + timedelta(days=30)) == []
    rows = point_in_time_training_rows(db, T0 + timedelta(days=60))
    assert len(rows) == 1
    assert rows[0]["is_fraud"] == 1
    assert rows[0]["feature_snapshot"]["velocity_1h"] == 11.0


def test_label_cannot_precede_decision() -> None:
    db = connect()
    append_decisions(db, [decision()])
    with pytest.raises(ValueError, match="before"):
        append_labels(
            db,
            [LabelEvent("lab-1", "dec-1", T0 - timedelta(seconds=1), True)],
        )


def test_population_stability_index() -> None:
    assert population_stability_index([1, 1, 1], [1, 1]) == 0.0
    assert population_stability_index(list(range(100)), list(range(60, 160))) > 0.1

