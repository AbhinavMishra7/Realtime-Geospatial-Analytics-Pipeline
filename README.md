# 🚗 Real-Time Geospatial Streaming Platform

A production-grade, event-driven streaming architecture for processing high-velocity geospatial telemetry at scale. Built on Apache Flink and Redpanda with dual-sinking to Redis (real-time) and MinIO (data lake).

## 🎯 Quick Summary

This repository implements a **Kappa Architecture** streaming platform using Apache Flink and Redpanda for real-time geospatial processing. It demonstrates production patterns including stateful processing, exactly-once semantics, event detection, and H3 indexing. Processes GPS telemetry from 20 drivers (10 pings/sec), scales to 10,000+ pings/sec, reconstructs trips, and detects anomalies.

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    PRODUCER (Mac)                           │
│  • 20 Simulated Drivers with Stateful Behavior            │
│  • Real-time GPS Pings (2s intervals)                     │
│  • Speed Calculation & Trip State Management              │
│  • 10 Events/second Output                                │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────┐
        │   KAFKA TOPIC              │
        │   gps_pings (Redpanda)     │
        │   • 20 Partitions          │
        │   • JSON Format            │
        └────────────────┬───────────┘
                         │
                         ▼
        ┌────────────────────────────┐
        │  FLINK PROCESSOR           │
        │  • SQL Transformations     │
        │  • Windowed Aggregations   │
        │  • Anomaly Detection       │
        │  • Stream Branching        │
        └────┬───────────────┬───────┘
             │               │
       ┌─────▼─────┐   ┌────▼────────┐
       │   REDIS   │   │   MINIO     │
       │ Real-time │   │  Data Lake  │
       │  Cache    │   │  Historical │
       ▼───────────┘   └─────────────┘
  • Live Dashboards       • ML Training
  • API Queries           • Compliance
  • Fleet Tracking        • Analytics
```

---

## ✨ Key Features

### 🔄 Stream Processing
- **Exactly-Once Semantics**: Distributed checkpointing ensures no data loss or duplication
- **Watermarking**: Handles 10-second late-arriving events gracefully
- **Stateful Processing**: Tracks individual driver behavior and trip states

### 🎯 Windowed Analytics
- **Tumbling Windows** (30s): Zone occupancy, speed statistics per region
- **Session Windows** (5min gap): Complete trip reconstruction from GPS pings
- **Custom Aggregations**: Real-time metrics and system health monitoring

### 🚨 Advanced Event Processing
- **Anomaly Detection**: Identifies impossible speeds, negative values, geofence violations
- **Trip State Machine**: available → on_trip → available transitions
- **H3 Geospatial Indexing**: Groups drivers into geographic hexagons (Uber's system)

### 💾 Dual-Sinking Pattern
- **Redis Sink**: Real-time cache with 60s TTL for live dashboards
- **MinIO Sink**: Partitioned data lake (year/month/day/hour) for analytics and ML

---

## 📋 Prerequisites

- **Docker & Docker Compose** (latest)
- **Python 3.10+** with uv package manager
- **macOS/Linux** (Windows WSL2 supported)
- **6GB+ Free Disk Space** (for containers and data)
- **4GB+ RAM** available

---

## 🚀 Quick Start

### 1. Clone & Setup
```bash
cd <project-directory>
git clone <repo-url> .
```

### 2. Install Local Dependencies
```bash
uv add confluent-kafka h3 redis boto3
```

### 3. Build & Start Containers
```bash
docker compose down  # Clean previous runs
docker compose up -d --build
```

### 4. Verify Services
```bash
# Check Kafka
docker logs redpanda | head -20

# Check Flink
docker logs flink-jobmanager | head -20

# Check Redis & MinIO are ready
docker logs redis | head -5
docker logs minio | head -5
```

### 5. Start Producer (in new terminal)
```bash
# Activate virtual environment (if using venv)
source venv/bin/activate  # macOS/Linux
# or
venv\Scripts\activate  # Windows

python producer.py
```

### 6. Monitor Output
```bash
docker logs -f flink-processor
```

---

## 📁 Project Structure

```
<project-directory>/
├── producer.py                      # GPS telemetry simulator (20 drivers)
├── processor.py                     # Flink SQL processing with windows & aggregations
├── processor_with_redis_minio.py   # Dual-sinking to Redis & MinIO (optional)
├── docker-compose.yml              # Full stack orchestration
├── Dockerfile                      # Flink container with dependencies
├── pyproject.toml                  # Python project config
├── README.md                       # This file
└── jars/
    └── flink-sql-connector-kafka-3.3.0-1.20.jar  # Kafka connector
```

---

## 🔍 Component Details

### **Producer.py** - Data Generation
Simulates 20 Uber drivers with realistic behavior:
- **Stateful tracking**: Position, status (available/on_trip), trip duration
- **Speed calculation**: Haversine formula for accurate distance
- **Trip lifecycle**: Random transitions between available and on_trip states
- **Output**: 10 GPS pings/second to Kafka topic `gps_pings`

**Configuration:**
```python
NUM_DRIVERS = 20           # Increase for higher throughput
H3_RESOLUTION = 9          # Geospatial grid detail level
UPDATE_INTERVAL = 2        # Seconds between pings per driver
```

### **processor.py** - Stream Processing
Advanced Flink SQL with production patterns:
- **Kafka Source**: Watermarked for handling late events
- **Custom UDFs**: H3 indexing, anomaly detection, trip categorization
- **Tumbling Windows**: 30-second metrics aggregation
- **Session Windows**: Trip reconstruction with 5-minute inactivity gaps

### **processor_with_redis_minio.py** - Dual-sinking processor (enhanced)
Stream-branching processor that enriches GPS pings and writes to Redis (real-time) and MinIO (data lake).

Key operational details:

- **Redis Sink (Real-time cache)**
  - Key: `driver:{driver_id}`
  - Value: minimal JSON (lat, lon, h3_cell, status, speed_kmh, ts_str, anomaly_type)
  - TTL: 60 seconds (keeps active driver set with 5min expiry)
- **MinIO Sink (Data Lake)**
  - Path pattern: `gps-data/year=YYYY/month=MM/day=DD/hour=HH/data_YYYYMMDD_HHMMSS.jsonl`
  - Format: JSONL (one JSON per line), batched (default batch size: 100 events)

New/expanded features in this processor (added recently):

- UDFs: `hex_index()` (H3), `detect_anomaly()` (flags NEGATIVE_SPEED / IMPOSSIBLE_SPEED), `trip_duration_category()`
- Windowed analytics: 30s tumbling window (zone metrics) and 1-min tumbling metrics for performance
- Session windows: 5-minute inactivity gap for trip reconstruction and trip metrics
- Anomaly side-stream: filtered stream of problematic events for alerting/monitoring
- Exactly-once semantics: checkpointing enabled (60s) and watermarking for 10s late events
- Dual-sinking pattern: real-time updates to Redis + batched writes to MinIO for archival and ML

Quick run (development):
```bash
# Start containers
docker compose up -d --build

# Start producer in a separate shell
python producer.py

# Run the processor locally (use this for testing without building images)
python processor_with_redis_minio.py

# Or view Flink logs if deployed as a container
docker logs -f flink-processor
```

Notes:
- Adjust Redis TTL or MinIO batch size in `processor_with_redis_minio.py` if needed.
- When deployed inside the Flink container, ensure the connector JARs are present in `/opt/flink/lib/`.

---

## 🎮 Usage Examples

### View Real-time Driver Locations (Redis)
```bash
docker exec -it redis redis-cli

# Get specific driver
> GET driver:driver_005

# List all active drivers
> SMEMBERS active_drivers

# Monitor all updates
> MONITOR
```

### Query Historical Data (MinIO)
```bash
# Access MinIO web UI
# Open: http://localhost:9001
# Login: minioadmin / minioadmin

# Or use AWS CLI
aws s3 ls s3://gps-data --endpoint-url http://localhost:9000 --recursive
```

### Monitor Flink Dashboard
```bash
# Open browser
# http://localhost:8081
```

---

## 🔑 Key Concepts

### 📊 Kappa Architecture
Stream-only design (no batch layer). All processing happens in real-time on streams.

```
Traditional Lambda:  Data → Batch + Speed → Serve
Kappa (This Project): Data → Stream (Batch included) → Serve  ✅
```

### 🌊 Stream Branching
Single input stream splits into multiple independent processing branches.

```
Input Stream
     ↓
     ├─→ Redis Sink (Real-time)
     ├─→ MinIO Sink (Historical)
     └─→ Console Sink (Monitoring)
```

### ⏱️ Watermarking
Signals "no more events older than X will arrive". Enables window closure.

```
Event with timestamp: 14:23:45
Watermark: 14:23:35 (10 seconds late)
Processing: ✓ Accepted
```

### 🪟 Windowing

**Tumbling** (non-overlapping):
```
[0-30s] [30-60s] [60-90s]
  ↓       ↓        ↓
 Emit    Emit     Emit
```

**Session** (gap-based):
```
[Events] ----5min gap---- [Events]
Session 1                 Session 2
```

### 📍 H3 Geospatial Indexing
Converts lat/lon to hierarchical hexagonal grid. Each resolution level shows different precision.

```
Resolution 0:   ~7000km hexagons (world view)
Resolution 5:   ~600km hexagons (country view)
Resolution 9:   ~175m hexagons (city blocks) ← Used here
Resolution 15:  ~1cm hexagons (ultra-precise)
```

---

## 📈 Performance Metrics

### Current Configuration
| Metric | Value |
|--------|-------|
| Drivers | 20 |
| GPS Pings/Second | 10 |
| Pings/Minute | 600 |
| Pings/Hour | 36,000 |
| Window Size | 30 seconds (tumbling) / 5 minutes (session) |
| Latency | < 100ms (end-to-end) |

### Scaling to Production
| Target | Drivers | Update Interval | Pings/Sec |
|--------|---------|-----------------|-----------|
| Current | 20 | 2s | 10 |
| 10x | 200 | 2s | 100 |
| 100x | 2,000 | 2s | 1,000 |
| 1000x | 20,000 | 2s | 10,000 |

---

## 🔧 Configuration & Customization

### Adjust Driver Count
```python
# In producer.py
NUM_DRIVERS = 100  # Increase for more throughput
```

### Adjust Update Frequency
```python
# In producer.py
UPDATE_INTERVAL = 1  # Ping every 1 second (2x throughput)
```

### Modify H3 Resolution
```python
# In producer.py
H3_RESOLUTION = 10  # Finer granularity (175m → 65m)
```

### Change Window Duration
```sql
-- In processor.py
TUMBLE(event_time, INTERVAL '60' SECOND)  -- 60s instead of 30s
SESSION(event_time, INTERVAL '10' MINUTE)  -- 10min instead of 5min
```

### Redis TTL
```python
# In processor_with_redis_minio.py (optional dual-sink setup)
self.redis_client.setex(cache_key, 120, value)  # 120 seconds instead of 60
```

---

## 🚨 Anomaly Detection Rules

| Anomaly | Condition | Action |
|---------|-----------|--------|
| **Impossible Speed** | speed > 120 km/h while "available" | Flag & route to alert stream |
| **Negative Speed** | speed < 0 | Data quality issue, investigate |
| **Geofence Violation** | Outside Delhi boundaries | Alert security team |
| **Long Idle** | No ping for 60+ seconds | Driver offline, trigger recovery |

---

## 📚 Data Flow Example

```
Time: 14:23:45

Producer Sends:
{
  "driver_id": "driver_005",
  "latitude": 28.6142,
  "longitude": 77.2089,
  "status": "on_trip",
  "speed_kmh": 42.5
}
    ↓
Kafka Queue (Persisted)
    ↓
Flink Processor Receives:
  1. Enriches with H3: "8928308304c1fff"
  2. Detects anomalies: "NORMAL"
  3. Calculates metrics for windows
    ↓
Stream Branch 1: Redis
  Key: driver:driver_005
  Cache Updated (60s TTL)
    ↓
Stream Branch 2: MinIO
  Written to JSONL batch
  Persisted when batch size = 100
    ↓
Dashboard Update
  Live map shows driver in hexagon 8928308304c1fff
  Real-time speed: 42.5 km/h
```

---

## 🧪 Testing & Validation

### Verify Producer
```bash
python producer.py
# Should see: ✓ Message delivered to gps_pings [partition X]
```

### Verify Kafka
```bash
docker exec -it redpanda rpk topic list
# Should show: gps_pings
```

### Verify Flink Processing
```bash
docker logs flink-processor
# Should see: ✓ Kafka source table created
# Should see: Processing GPS data...
```

### Verify Redis Updates
```bash
docker exec -it redis redis-cli KEYS "driver:*" | wc -l
# Should show: 20 (one key per driver)
```

### Verify MinIO Data
```bash
docker exec -it minio mc ls minio/gps-data
# Should show: year=2026/month=05/day=17/...
```

---

## 🐛 Troubleshooting

### "Waiting 20 seconds for Flink cluster to stabilize..."
**Normal behavior** - Flink JobManager and TaskManager need time to initialize. Wait 40+ seconds for output.

### "Cannot connect to Kafka"
```bash
docker compose ps  # Verify all services running
docker logs redpanda  # Check for startup errors
docker compose restart redpanda
```

### "Redis connection refused"
```bash
docker compose restart redis
docker exec redis redis-cli PING  # Should return PONG
```

### Producer sends but processor doesn't receive
```bash
# Check Kafka topic has data
docker exec -it redpanda rpk topic consume gps_pings --offset :start

# Check Flink logs
docker logs flink-processor | grep -i "error"
```

---

## 📖 Further Learning

- **Code Comments** - Detailed explanations within each file:
  - Stateful processing concepts in producer.py
  - Windowing strategies in processor files
  - Exactly-once semantics configuration
  - Complex event processing logic
  - Stream branching patterns

---

## 🚀 Future Enhancements

- [ ] ML model integration for demand prediction
- [ ] Join with traffic data for route optimization
- [ ] PostgreSQL sink for long-term analytics
- [ ] GraphQL API for real-time driver queries
- [ ] Multi-region deployment with state replication
- [ ] Custom metric exporters for Prometheus
- [ ] Real passenger data integration
- [ ] Auto-scaling with Kubernetes

---

## 📝 License

MIT License - See LICENSE file for details

---

## 💬 Questions?

Refer to:
1. **Code comments** in each file for streaming concepts
2. **Docker logs** for real-time diagnostics
3. **Configuration sections** above for customization options
4. **Troubleshooting section** for common issues

---

**Built with ❤️ using Apache Flink, Redpanda, Redis, and MinIO**
