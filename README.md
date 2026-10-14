# Real-Time Fraud Decision Feedback Loop

An executable reference for the **asynchronous learning plane** behind a real-time fraud gateway. It preserves each authorization decision with its point-in-time feature snapshot, joins only labels that were observable at the evaluation date, and exposes drift and policy metrics without contaminating the payment path.

The companion synchronous gateway is [ShieldOS](https://github.com/sandipdikshit/ShieldOS-Fraud-Decisioning).

## Workflow

```text
Payment gateway decision
        │
        ├── immutable decision event ──► decision_event
        │                               score · action · versions
        │                               feature snapshot · audit digest
        │
        └── customer outcome (later) ──► label_event
                                        chargeback · review disposition
                                                    │
                                                    ▼
                                      point-in-time maturity join
                                                    │
                         ┌──────────────────────────┼─────────────────────┐
                         ▼                          ▼                     ▼
                 policy monitoring          drift / skew          retraining set
                 recall · precision         score PSI             no future labels
                         │                          │                     │
                         └──────────────────────────┴──────────────┬──────┘
                                                                  ▼
                                                        champion/challenger
                                                        shadow → canary
```

## What is implemented

- immutable, idempotent decision ingestion;
- versioned model and policy identifiers on every row;
- a canonical feature snapshot and reproducible audit digest;
- referentially safe delayed-label ingestion;
- an as-of join that blocks future-label leakage;
- policy precision, recall, prevalence, and block-rate metrics;
- population stability index for feature or score drift;
- an executed notebook using an authentic anonymized OpenML fraud-data extract;
- unit tests for replay safety, temporal leakage, label ordering, and drift.

`feedback_store.py` uses SQLite so the workflow runs locally with no services. A production deployment maps the same contracts to Kafka, an object-store table format, and an analytical warehouse. SQLite is an execution surrogate, not a throughput claim.

## Evidence boundary

The checked-in extract contains 3,000 rows from [OpenML dataset 1597](https://www.openml.org/search?id=1597&type=data), including all 492 observed fraud labels. The source contains anonymized PCA features, transaction-relative time, amount, and class. It does not contain chargeback arrival times, policy decisions, model scores, customer IDs, device IDs, IMSI data, or investigator outcomes.

The notebook therefore labels two layers explicitly:

| Layer | Status |
|---|---|
| Amount, relative transaction order, anonymized V-features, fraud label | Observed public data |
| Calendar timestamps, decision score/action, label delay, entity reference | Deterministic simulation |

## Repository layout

```text
.
├── README.md
├── feedback_store.py
├── notebooks/
│   └── Fraud_Decision_Feedback_Loop.ipynb
├── data/
│   ├── creditcard_openml_1597_extract.csv.gz
│   └── creditcard_openml_1597_manifest.json
├── tests/
│   └── test_feedback_store.py
└── requirements.txt
```

## Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest -q
jupyter notebook notebooks/Fraud_Decision_Feedback_Loop.ipynb
```

## Production mapping

| Local component | Production analogue |
|---|---|
| `decision_event` table | compacted Kafka event plus append-only warehouse fact |
| JSON feature snapshot | schema-versioned Avro/Protobuf payload |
| SQLite transaction | transactional outbox and idempotent consumer |
| `label_event` table | chargeback, refund, and investigator-disposition stream |
| as-of SQL join | point-in-time Iceberg/Delta/Snowflake transformation |
| PSI function | scheduled feature, score, and segment drift monitor |
| notebook gate | model registry validation and canary promotion check |

Operational controls that belong around this core include schema compatibility, dead-letter handling, freshness SLAs, replay checkpoints, PII tokenization, row-level access, retention rules, and signed lineage metadata.

## Design note

The architecture uses standard event-sourcing and delayed-label patterns. It was implemented independently after reviewing several public fraud-pipeline repositories; no third-party source code was copied into this repository.

