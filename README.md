# 🚗 Real-Time Geospatial Streaming Platform

A production-grade, event-driven streaming architecture for processing high-velocity geospatial telemetry at scale. Built on Apache Flink and Redpanda with dual-sinking to Redis (real-time) and MinIO (data lake).

## 🎯 Quick Summary

This repository implements a **Kappa Architecture** streaming platform using Apache Flink and Redpanda for real-time geospatial processing. It demonstrates production patterns including stateful processing, exactly-once semantics, event detection, and H3 indexing. It simulates 20 drivers and supports a pipeline that enriches GPS pings, detects anomalies, and writes both live and historical outputs.

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    PRODUCER                              │
│  • 20 Simulated Drivers with Stateful Behavior             │
│  • Real-time GPS Pings (2–5s intervals)                    │
│  • Speed calculation, trip status, and message publishing   │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────┐
        │   Kafka Topic                │
        │   gps_pings (Redpanda)       │
        │   • JSON format              │
        └────────────────┬───────────┘
                         │
                         ▼
        ┌────────────────────────────┐
        │  Flink Processor             │
        │  • SQL enrichment            │
        │  • Windowed analytics        │
        │  • Custom UDFs               │
        │  • Dual sinking              │
        └────┬───────────────┬───────┘
             │               │
       ┌─────▼─────┐   ┌────▼────────┐
       │   REDIS   │   │   MINIO     │
       │ Real-time │   │  Data Lake  │
       │  Cache    │   │ Historical  │
       ▼───────────┘   └─────────────┘
  • Live dashboards      • Analytics / ML
  • Low-latency reads    • Historical storage
```

---

## ✨ Key Features

### 🔄 Stream Processing
- **Exactly-once semantics**: Flink checkpointing supports consistent processing.
- **Watermarking**: 10-second watermark handles late events.
- **Stateful processing**: Per-driver state, trip reconstruction, and session windows.

### 🎯 Windowed Analytics
- **Tumbling windows** (30s): zone occupancy and speed metrics.
- **Session windows** (5-minute gap): trip reconstruction and journey summarization.
- **Performance metrics**: 1-minute monitoring snapshots.

### 🚨 Advanced Event Processing
- **Anomaly detection**: impossible speed and negative speed.
- **Trip state machine**: `available` → `on_trip` → `available`.
- **H3 geospatial indexing**: groups locations into hexagonal zones.

### 💾 Dual-Sinking Pattern
- **Redis sink**: real-time cache with 60-second TTL.
- **MinIO sink**: data lake storage partitioned by year/month/day/hour.

---

## 📋 Prerequisites

- **Docker & Docker Compose**
- **Python 3.11**
- **macOS/Linux** (Windows WSL2 supported)
- **6GB+ free disk space**
- **4GB+ RAM**

---

## 🚀 Quick Start

### 1. Clone the repo
```bash
cd <project-directory>
git clone <repo-url> .
```

### 2. Install local dependencies
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install boto3 confluent-kafka h3 redis
```

### 3. Build and start the stack
```bash
docker compose down
docker compose up -d --build
```

### 4. Verify service startup
```bash
docker logs redpanda | head -20
docker logs flink-jobmanager | head -20
docker logs redis | head -5
docker logs minio | head -5
```

### 5. Start the producer
```bash
source .venv/bin/activate
python producer.py
```

### 6. Monitor the processor
```bash
docker logs -f flink-processor
```

---

## 📁 Project Structure

```
<project-directory>/
├── producer.py                      # GPS telemetry simulator
├── processor.py                     # Flink SQL reference pipeline
├── processor_with_redis_minio.py    # Dual-sink Flink processor
├── docker-compose.yml               # Stack orchestration
├── Dockerfile                       # Flink container dependencies
├── pyproject.toml                   # Python metadata
├── README.md                        # Project documentation
├── ARCHITECTURE_AND_DECISIONS.md    # Architecture rationale
└── jars/
    └── flink-sql-connector-kafka-3.4.0-1.20.jar  # Kafka connector
```

---

## 🔍 Component Details

### **producer.py**
Simulates GPS telemetry for a fleet of drivers:
- Stateful driver simulation
- Speed calculation using Haversine distance
- Trip start/stop transitions
- JSON output to `gps_pings`

### **processor.py**
A reference Flink pipeline demonstrating core streaming logic:
- Kafka source with watermarking
- UDF-based enrichment
- Tumbling and session windows
- Aggregations and metrics

### **processor_with_redis_minio.py**
The main streaming pipeline in this repository:
- consumes from Redpanda
- enriches with H3 and anomaly flags
- writes live state to Redis
- writes historical batches to MinIO
- prints console output for monitoring

Key behavior:
- **Redis sink**: `driver:{driver_id}` keys with 60s TTL
- **MinIO sink**: JSONL objects partitioned by `year/month/day/hour`
- **Branching**: live cache + archival data + monitoring

---

## 🔧 Docker Compose and Networking

The stack runs on Docker Compose with a custom network `kappa-network`.
Service names are used for internal container communication:

- `redpanda:9092` for Kafka
- `flink-jobmanager:8081` for Flink REST
- `redis:6379` for Redis
- `minio:9000` for MinIO S3 API

### Exposed ports

- `19092:19092` → Redpanda external Kafka endpoint
- `8081:8081` → Flink JobManager UI
- `8080:8080` → Redpanda Console UI
- `9000:9000` → MinIO S3 API
- `9001:9001` → MinIO Console UI
- `6379:6379` → Redis

---

## 🎯 Why this stack?

### Why Apache Flink?
- true event-at-a-time stream processing
- strong event-time and window support
- stateful stream processing
- exactly-once semantics

### Why Redpanda?
- Kafka-compatible
- easier Docker deployment than ZooKeeper-based Kafka
- reliable for local development

### Why Redis + MinIO?
- Redis for low-latency live state
- MinIO for historical data lake storage
- both are easy to swap with cloud services later

---

## 🎯 Usage and Validation

### Verify producer delivery
```bash
python producer.py
```
Should show repeated `✅ Message delivered` output.

### Verify Kafka topic
```bash
docker exec -it redpanda rpk topic list
```
Should list `gps_pings`.

### Verify Flink processing
```bash
docker logs flink-processor | grep -i "✓"
```
Should show successful pipeline initialization.

### Verify Redis data
```bash
docker exec -it redis redis-cli KEYS "driver:*"
```
Should return driver keys.

### Verify MinIO data
```bash
docker exec -it minio mc ls minio/gps-data
```
Should show partitioned objects.

---

## 🧠 Concepts to understand

### Kappa architecture
A single streaming pipeline handles both real-time and historical workloads.

### Stream branching
One enriched stream feeds multiple output sinks.

### Event-time and watermarks
Flink uses event-time and a 10s watermark to manage late-arriving GPS telemetry.

### Session windows
Trips are grouped using a 5-minute inactivity gap.

---

## 🐛 Troubleshooting

### Job submission fails with `Connection refused: /0.0.0.0:8081`
The processor must use `flink-jobmanager:8081` inside Docker, not localhost.

### No job visible in Flink UI
- ensure Docker compose is running
- wait 30–40 seconds for Flink startup
- confirm producer is sending data
- inspect `docker logs flink-processor`

### Redis or MinIO connectivity issues
Check that the corresponding container is running and that service names match the compose file.

---

## ✅ Notes

- `processor_with_redis_minio.py` is the active pipeline for this project.
- `processor.py` remains a reference implementation.
- The Dockerfile installs Python libraries needed by the Flink processor.
- The `jars/` directory contains the Kafka connector required for the Flink job.

---

## 🚀 Future enhancements

- Add geofence alerting and demand prediction
- Add Prometheus/Grafana monitoring
- Add Kafka schema registry and validation
- Add cloud object storage support (AWS S3 / GCS)
- Add Kubernetes deployment

---

## 💬 Questions?

- See code comments in `producer.py` and `processor_with_redis_minio.py`.
- Use Docker logs for runtime diagnostics.

---

**Built with Apache Flink, Redpanda, Redis, and MinIO**
