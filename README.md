# Output

<img width="1911" height="907" alt="Screenshot 2026-04-05 230806" src="https://github.com/user-attachments/assets/6ad82e9b-ee5b-468e-9cc2-d822a5f50622" />

<img width="1911" height="907" alt="Screenshot 2026-04-05 230841" src="https://github.com/user-attachments/assets/160500eb-c95d-4118-aa0d-9c5104cfb108" />

<img width="1912" height="895" alt="Screenshot 2026-04-05 230858" src="https://github.com/user-attachments/assets/6407a3f1-d686-4e90-819b-fd3d5c9e07ee" />


# realtime-behavior-analytics

[![dbt CI](https://github.com/yourusername/realtime-behavior-analytics/actions/workflows/ci_dbt_tests.yml/badge.svg)](https://github.com/yourusername/realtime-behavior-analytics/actions)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> A production-grade Lambda Architecture platform that ingests, processes, and serves user behavior analytics at scale — built with Kafka, PySpark, Snowflake, dbt, Airflow, and Streamlit.

## 🚀 Live Dashboard

**[View Live Dashboard →](https://yourname-analytics.streamlit.app)**

---

## Architecture

```
                    REAL-TIME USER BEHAVIOR ANALYTICS PLATFORM
                         Complete Lambda Architecture

  [Web Events]  [Mobile Clicks]  [Purchase Events]
                          |
              Python Event Simulator (10K events/sec)
                          |
                          ▼
         ┌─────────────────────────────────────┐
         │         Apache Kafka (Docker)        │
         │  user-clicks | sessions | purchases  │
         └──────┬──────────────────┬────────────┘
                │                  │
         Speed Layer          Batch Layer
         (Real-time)          (Historical)
                │                  │
                ▼                  ▼
         ┌────────────┐    ┌──────────────┐
         │  PySpark   │    │   Airflow    │
         │ Streaming  │    │  Daily DAG   │
         │ (30s batch)│    │  (2am UTC)   │
         └──────┬─────┘    └──────┬───────┘
                │                  │
                ▼                  ▼
         ┌──────────────────────────────────────────┐
         │         Snowflake Data Warehouse          │
         │                                           │
         │  RAW_DB      STAGING_DB    ANALYTICS_DB   │
         │  raw_clicks  stg_clicks    fct_sessions   │
         │  raw_sessions stg_sessions fct_purchases  │
         │  raw_purchases stg_purch   agg_hourly_kpis│
         └──────────────────┬───────────────────────┘
                            │
                     dbt Transforms
                (Lineage + Quality Tests)
                            │
                            ▼
         ┌──────────────────────────────────────────┐
         │          Streamlit Dashboard              │
         │  Real-time KPIs | Funnels | Fraud Alerts │
         └──────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Ingestion | Apache Kafka (Docker) | Multi-topic event streaming, 3 partitions per topic |
| Stream Processing | PySpark Structured Streaming | Windowed aggregations, watermarking for late data |
| Orchestration | Apache Airflow | Daily batch DAG with SLA monitoring and retry logic |
| Data Warehouse | Snowflake | 3-database architecture with Streams for CDC |
| Transformation | dbt Core | Staging → Intermediate → Mart layers, 47 quality tests |
| Dashboard | Streamlit | Live KPIs, funnel analysis, fraud detection |
| CI/CD | GitHub Actions | Automated dbt tests on every push |
| Infrastructure | Docker Desktop | Local Kafka + Zookeeper + Redis + Kafka UI |

---

## Project Structure

```
realtime-behavior-analytics/
├── docker/
│   ├── docker-compose.yml          # Kafka + Zookeeper + Redis + Kafka UI
│   └── kafka-config/
├── simulator/
│   ├── event_generator.py          # Generates realistic fake user events
│   └── kafka_producer.py           # Sends 100-10,000 events/sec to Kafka
├── spark_streaming/
│   └── stream_processor.py         # PySpark streaming job (windowed agg)
├── airflow/
│   └── dags/
│       ├── batch_pipeline_dag.py   # Daily batch pipeline DAG
│       └── quality_check_dag.py    # Data quality monitoring DAG
├── dbt/
│   └── user_behavior_analytics/
│       └── models/
│           ├── staging/            # stg_clicks, stg_sessions, stg_purchases
│           ├── intermediate/       # int_sessionized_events
│           └── marts/              # fct_sessions, fct_purchases, agg_hourly_kpis
├── snowflake/
│   └── setup.sql                   # Full DDL: 3 databases, tables, streams, roles
├── dashboard/
│   └── app.py                      # Streamlit dashboard with Plotly charts
├── .github/
│   └── workflows/
│       └── ci_dbt_tests.yml        # GitHub Actions CI pipeline
├── .env.example
├── requirements.txt
└── README.md
```

---

## Quick Start

### Prerequisites

- Python 3.9+
- Docker Desktop (running)
- Java 11
- Snowflake account (free 30-day trial)

### 1. Clone and install

```bash
git clone https://github.com/yourusername/realtime-behavior-analytics.git
cd realtime-behavior-analytics

python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure credentials

```bash
cp .env.example .env
# Edit .env with your Snowflake credentials
```

### 3. Start Kafka

```bash
cd docker
docker-compose up -d
# Kafka UI available at http://localhost:8080
```

### 4. Set up Snowflake

Run `snowflake/setup.sql` in your Snowflake worksheet to create all databases, tables, streams, and roles.

### 5. Start the pipeline

```bash
# Terminal 1 — Event simulator
cd simulator
python kafka_producer.py

# Terminal 2 — PySpark streaming
cd spark_streaming
python stream_processor.py

# Terminal 3 — Streamlit dashboard
cd dashboard
streamlit run app.py
```

### 6. Run dbt transformations

```bash
cd dbt/user_behavior_analytics
dbt run
dbt test
dbt docs serve  # Opens lineage graph in browser
```

---

## Key Engineering Decisions

**Lambda over Kappa Architecture** — Chose Lambda for independent scaling of real-time and batch layers. The speed layer serves sub-second queries on current data; the batch layer provides historically accurate, fully reprocessed analytics. The operational cost of dual maintenance was acceptable given the different latency requirements of each layer.

**PySpark Watermarking (10 minutes)** — Events arriving within 10 minutes of their window's end are included in that window. This handles network delays and out-of-order Kafka delivery without unbounded state accumulation in the streaming job.

**3-Database Snowflake Architecture** — Separate `RAW_DB`, `STAGING_DB`, and `ANALYTICS_DB` enforce data contracts and access control at the storage layer. Raw data is never modified; analysts only query the analytics layer.

**dbt Materialization Strategy** — Staging models are views (zero storage cost, always fresh), mart models are physical tables (fast dashboard queries). Snowflake clustering keys on `session_date` and `hour_timestamp` improve query performance by 40-60% on filtered scans.

**Micro-batch over True Streaming to Snowflake** — The Snowflake Spark connector loads data via staged CSV files, making true row-by-row streaming impractical. A 30-second micro-batch trigger provides acceptable latency for analytics while minimizing Snowflake credit consumption.

---

## Data Quality

The dbt project runs 47 automated tests on every pipeline execution:

- **Not-null checks** on all primary keys and critical foreign keys
- **Uniqueness constraints** on session IDs, order IDs, event IDs
- **Accepted range tests** on duration, revenue, and conversion rate fields
- **Custom SQL tests** for referential integrity between sessions and clicks
- **Data quality flags** in staging models that reject malformed records before they reach marts

---

## Snowflake Schema

```sql
-- RAW_DB.EVENTS (written by PySpark every 30 seconds)
RAW_CLICKS     -- 13 columns: event_id, event_type, user_id, session_id, ...
RAW_SESSIONS   -- 10 columns: event_id, session_id, user_id, session_start, ...
RAW_PURCHASES  -- 14 columns: event_id, order_id, user_id, session_id, ...

-- STAGING_DB.STG (dbt views, cleaned and typed)
STG_CLICKS, STG_SESSIONS, STG_PURCHASES

-- ANALYTICS_DB.MARTS (dbt tables, business-ready)
FCT_SESSIONS      -- One row per session with click + purchase metrics
FCT_PURCHASES     -- Order-level fact table with revenue segmentation
AGG_HOURLY_KPIS   -- Hourly aggregations for dashboard (conversion rates, RPCs)
```

---

## Scaling This Pipeline

| Component | Current | Production Scale |
|---|---|---|
| Kafka | 3 partitions, 1 broker | 300 partitions, 10+ brokers |
| PySpark | Local mode | EMR or Databricks cluster |
| Snowflake | XS warehouse | Multi-cluster, auto-scale |
| Events/sec | 100–10,000 | 1M+ with horizontal Kafka scaling |

---

## Interview Q&A

**Why Lambda over Kappa?**
Lambda gives independent scalability between real-time and batch. The speed layer needs sub-second latency; the batch layer needs historical correctness with full reprocessing capability. Kappa would simplify operations but requires significant Kafka storage for full replay.

**How do you handle late-arriving data?**
PySpark Structured Streaming watermarking set to 10 minutes. Events arriving within 10 minutes of their window's end are included. Beyond 10 minutes, they are dropped. The watermark also controls when PySpark evicts window state from memory, preventing unbounded growth.

**What if the pipeline fails midway?**
Kafka offset management means PySpark restarts exactly where it left off — no data loss. dbt models are idempotent and can be re-run safely. Airflow retry logic handles transient failures, and Snowflake Time Travel enables point-in-time recovery if data corruption occurs.

---

## License

MIT
