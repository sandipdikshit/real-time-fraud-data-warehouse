"""Point-in-time fraud decision and delayed-label feedback store.

The module is intentionally small enough to review in an interview. SQLite is
the local execution target; the contracts map directly to an append-only event
topic plus warehouse fact tables in a distributed deployment.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Iterable, Mapping, Sequence


UTC = timezone.utc
VALID_ACTIONS = {"APPROVE", "STEP_UP", "HARD_BLOCK"}


def utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def canonical_json(value: Mapping[str, float]) -> str:
    return json.dumps(dict(value), sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class DecisionEvent:
    decision_id: str
    event_ts: datetime
    entity_ref: str
    amount: float
    risk_score: float
    action: str
    model_version: str
    policy_version: str
    features: Mapping[str, float]

    def validate(self) -> None:
        if not self.decision_id or not self.entity_ref:
            raise ValueError("decision_id and entity_ref are required")
        utc_iso(self.event_ts)
        if self.amount < 0 or not math.isfinite(self.amount):
            raise ValueError("amount must be finite and non-negative")
        if not 0 <= self.risk_score <= 100:
            raise ValueError("risk_score must be between 0 and 100")
        if self.action not in VALID_ACTIONS:
            raise ValueError(f"unsupported action: {self.action}")

    @property
    def audit_digest(self) -> str:
        payload = asdict(self)
        payload["event_ts"] = utc_iso(self.event_ts)
        payload["features"] = json.loads(canonical_json(self.features))
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class LabelEvent:
    label_id: str
    decision_id: str
    observed_ts: datetime
    is_fraud: bool
    source: str = "chargeback"

    def validate(self) -> None:
        if not self.label_id or not self.decision_id:
            raise ValueError("label_id and decision_id are required")
        utc_iso(self.observed_ts)


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS decision_event (
    decision_id TEXT PRIMARY KEY,
    event_ts TEXT NOT NULL,
    entity_ref TEXT NOT NULL,
    amount REAL NOT NULL CHECK (amount >= 0),
    risk_score REAL NOT NULL CHECK (risk_score BETWEEN 0 AND 100),
    action TEXT NOT NULL CHECK (action IN ('APPROVE', 'STEP_UP', 'HARD_BLOCK')),
    model_version TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    feature_snapshot TEXT NOT NULL,
    audit_digest TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS label_event (
    label_id TEXT PRIMARY KEY,
    decision_id TEXT NOT NULL REFERENCES decision_event(decision_id),
    observed_ts TEXT NOT NULL,
    is_fraud INTEGER NOT NULL CHECK (is_fraud IN (0, 1)),
    source TEXT NOT NULL,
    UNIQUE (decision_id, source)
);

CREATE INDEX IF NOT EXISTS idx_decision_event_ts ON decision_event(event_ts);
CREATE INDEX IF NOT EXISTS idx_label_observed_ts ON label_event(observed_ts);
"""


def connect(path: str = ":memory:") -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.executescript(SCHEMA)
    return connection


def append_decisions(connection: sqlite3.Connection, events: Iterable[DecisionEvent]) -> int:
    inserted = 0
    for event in events:
        event.validate()
        row = (
                event.decision_id,
                utc_iso(event.event_ts),
                event.entity_ref,
                event.amount,
                event.risk_score,
                event.action,
                event.model_version,
                event.policy_version,
                canonical_json(event.features),
                event.audit_digest,
            )
        try:
            with connection:
                connection.execute(
                    "INSERT INTO decision_event VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    row,
                )
            inserted += 1
        except sqlite3.IntegrityError as exc:
            existing = connection.execute(
                "SELECT audit_digest FROM decision_event WHERE decision_id = ?",
                (event.decision_id,),
            ).fetchone()
            if existing is None or existing["audit_digest"] != event.audit_digest:
                raise ValueError(f"conflicting replay for {event.decision_id}") from exc
    return inserted


def append_labels(connection: sqlite3.Connection, events: Iterable[LabelEvent]) -> int:
    rows = []
    for event in events:
        event.validate()
        decision = connection.execute(
            "SELECT event_ts FROM decision_event WHERE decision_id = ?", (event.decision_id,)
        ).fetchone()
        if decision is None:
            raise ValueError(f"unknown decision_id: {event.decision_id}")
        if utc_iso(event.observed_ts) < decision["event_ts"]:
            raise ValueError("a label cannot be observed before its decision")
        rows.append(
            (
                event.label_id,
                event.decision_id,
                utc_iso(event.observed_ts),
                int(event.is_fraud),
                event.source,
            )
        )
    before = connection.total_changes
    with connection:
        connection.executemany(
            """INSERT OR IGNORE INTO label_event VALUES (?, ?, ?, ?, ?)""", rows
        )
    return connection.total_changes - before


def point_in_time_training_rows(
    connection: sqlite3.Connection, as_of: datetime
) -> list[dict[str, object]]:
    """Return only decisions whose labels were observable by ``as_of``."""
    cutoff = utc_iso(as_of)
    rows = connection.execute(
        """
        SELECT d.*, l.is_fraud, l.observed_ts, l.source AS label_source
        FROM decision_event AS d
        JOIN label_event AS l USING (decision_id)
        WHERE d.event_ts <= ? AND l.observed_ts <= ?
        ORDER BY d.event_ts, d.decision_id
        """,
        (cutoff, cutoff),
    ).fetchall()
    result: list[dict[str, object]] = []
    for row in rows:
        item = dict(row)
        item["feature_snapshot"] = json.loads(str(item["feature_snapshot"]))
        result.append(item)
    return result


def population_stability_index(
    reference: Sequence[float], current: Sequence[float], bins: int = 10
) -> float:
    """Calculate equal-width PSI with bounded, deterministic binning."""
    if bins < 2 or not reference or not current:
        raise ValueError("PSI requires two non-empty samples and at least two bins")
    lo = min(min(reference), min(current))
    hi = max(max(reference), max(current))
    if lo == hi:
        return 0.0
    width = (hi - lo) / bins

    def proportions(values: Sequence[float]) -> list[float]:
        counts = [0] * bins
        for value in values:
            index = min(int((value - lo) / width), bins - 1)
            counts[index] += 1
        epsilon = 1e-6
        total = len(values) + epsilon * bins
        return [(count + epsilon) / total for count in counts]

    expected = proportions(reference)
    actual = proportions(current)
    return sum((a - e) * math.log(a / e) for e, a in zip(expected, actual))


def policy_metrics(rows: Sequence[Mapping[str, object]]) -> dict[str, float]:
    if not rows:
        raise ValueError("at least one matured row is required")
    fraud = [bool(row["is_fraud"]) for row in rows]
    blocked = [str(row["action"]) == "HARD_BLOCK" for row in rows]
    positives = sum(fraud)
    predicted = sum(blocked)
    true_positives = sum(y and p for y, p in zip(fraud, blocked))
    return {
        "matured_rows": float(len(rows)),
        "fraud_prevalence": positives / len(rows),
        "block_rate": predicted / len(rows),
        "block_precision": true_positives / predicted if predicted else 0.0,
        "fraud_recall": true_positives / positives if positives else 0.0,
    }


__all__ = [
    "DecisionEvent",
    "LabelEvent",
    "append_decisions",
    "append_labels",
    "connect",
    "point_in_time_training_rows",
    "policy_metrics",
    "population_stability_index",
]

