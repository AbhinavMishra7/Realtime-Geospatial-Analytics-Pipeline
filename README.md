# Realtime-Geospatial-Analytics-Pipeline
A Kappa Architecture streaming platform using Apache Flink and Redpanda for real-time geospatial processing. Demonstrates production patterns: stateful processing, exactly-once semantics, event detection, and H3 indexing. Processes GPS telemetry from 20 drivers (10 pings/sec), scales to 10,000+ pings/sec, reconstructs trips, detects anomalies.
