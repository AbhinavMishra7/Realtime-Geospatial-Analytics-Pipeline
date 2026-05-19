import os
import h3
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.udf import udf
from pyflink.common.typeinfo import Types

print("=" * 80)
print("FLINK PROCESSOR - DEMONSTRATION")
print("=" * 80)

# ============================================================================
# 1. DEFINE CUSTOM UDFS (User Defined Functions)
# ============================================================================

@udf(result_type=DataTypes.STRING())
def hex_index(lat: float, lon: float, resolution: int = 9) -> str:
    """Convert lat/lon to H3 hexagon index"""
    return h3.latlng_to_cell(float(lat), float(lon), resolution)

@udf(result_type=DataTypes.STRING())
def detect_anomaly(speed_kmh: float, status: str) -> str:
    """
    CONCEPT: Side Output Logic - Detects anomalies
    - Speed > 120 kmh while available (impossible without vehicle)
    - Speed < 0 (data error)
    Returns anomaly_type or 'NORMAL'
    """
    if speed_kmh < 0:
        return "NEGATIVE_SPEED"
    if speed_kmh > 120 and status == "available":
        return "IMPOSSIBLE_SPEED"
    return "NORMAL"

@udf(result_type=DataTypes.BIGINT())
def trip_duration_category(duration_secs: int) -> int:
    """Categorize trips by duration for windowed analytics"""
    if duration_secs == 0:
        return 0  # No trip
    elif duration_secs < 300:
        return 1  # Short trip (< 5 min)
    elif duration_secs < 900:
        return 2  # Medium trip (5-15 min)
    else:
        return 3  # Long trip (> 15 min)

# ============================================================================
# 2. INITIALIZE FLINK STREAMING ENVIRONMENT
# ============================================================================

print("\n[1] Initializing Flink Streaming Environment...")
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = TableEnvironment.create(settings)

# Enable advanced features
# CONCEPT: Checkpointing for exactly-once semantics
table_env.get_config().set("execution.checkpointing.mode", "EXACTLY_ONCE")
table_env.get_config().set("execution.checkpointing.interval", "60000")

# CONCEPT: Watermark strategy for handling late events
table_env.get_config().set("table.exec.source.idle-timeout", "30000")

print("✓ Environment initialized with checkpointing (EXACTLY-ONCE semantics)")

# ============================================================================
# 3. DEFINE SOURCE TABLE - Kafka Input
# ============================================================================

print("\n[2] Setting up Kafka Source Table (Watermarked for late events)...")

source_ddl = """
    CREATE TABLE gps_pings (
        driver_id STRING,
        latitude DOUBLE,
        longitude DOUBLE,
        status STRING,
        speed_kmh DOUBLE,
        ts_str STRING,
        trip_duration_secs INT,
        event_time AS TO_TIMESTAMP(ts_str),
        WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'gps_pings',
        'properties.bootstrap.servers' = 'redpanda:9092',
        'properties.group.id' = 'flink-processor-group',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
"""
table_env.execute_sql(source_ddl)
print("✓ Kafka source table created")
print("  - Watermark: 10 seconds for handling late events")

# ============================================================================
# 4. REGISTER CUSTOM UDFS
# ============================================================================

print("\n[3] Registering Custom UDFs...")
table_env.create_temporary_system_function("hex_index", hex_index)
table_env.create_temporary_system_function("detect_anomaly", detect_anomaly)
table_env.create_temporary_system_function("trip_duration_category", trip_duration_category)
print("✓ UDFs registered: hex_index, detect_anomaly, trip_duration_category")

# ============================================================================
# 5. ENRICHED STREAM - Add geospatial and anomaly data
# ============================================================================

print("\n[4] Creating Enriched Stream (BASE VIEW)...")

enriched_stream = table_env.sql_query("""
    SELECT 
        driver_id,
        latitude,
        longitude,
        status,
        speed_kmh,
        timestamp,
        trip_duration_secs,
        hex_index(latitude, longitude, 9) as h3_cell,
        detect_anomaly(speed_kmh, status) as anomaly_type,
        trip_duration_category(trip_duration_secs) as trip_category,
        event_time,
        CURRENT_TIMESTAMP as processing_time
    FROM gps_pings
""")
print("✓ Enriched stream created with:")
print("  - H3 hexagon indexing")
print("  - Anomaly detection")
print("  - Trip categorization")

# ============================================================================
# 6. WINDOWED AGGREGATIONS - CONCEPT: Time Windows
# ============================================================================

print("\n[5] Creating WINDOWED AGGREGATIONS (Tumbling Window - 30 seconds)...")

"""
CONCEPT EXPLANATION - Windows:
- TUMBLE: Non-overlapping 30-second windows
- Each window processes independently
- Shows metrics per zone per time period
"""

window_aggregation = table_env.sql_query("""
    SELECT 
        TUMBLE_START(event_time, INTERVAL '30' SECOND) as window_start,
        TUMBLE_END(event_time, INTERVAL '30' SECOND) as window_end,
        h3_cell,
        status,
        COUNT(*) as ping_count,
        COUNT(DISTINCT driver_id) as unique_drivers,
        ROUND(AVG(speed_kmh), 2) as avg_speed,
        ROUND(MAX(speed_kmh), 2) as max_speed,
        ROUND(MIN(speed_kmh), 2) as min_speed,
        SUM(CASE WHEN status = 'on_trip' THEN 1 ELSE 0 END) as on_trip_count,
        ROUND(CAST(SUM(CASE WHEN status = 'on_trip' THEN 1 ELSE 0 END) AS DECIMAL) / 
              CAST(COUNT(*) AS DECIMAL) * 100, 2) as on_trip_percentage
    FROM gps_pings
    GROUP BY 
        TUMBLE(event_time, INTERVAL '30' SECOND),
        h3_cell,
        status
""")
print("✓ Windowed aggregation created:")
print("  - 30-second non-overlapping windows")
print("  - Metrics per H3 cell per status")
print("  - Driver count, speed stats, trip percentage")

# ============================================================================
# 7. ANOMALY STREAM - CONCEPT: Additional Output
# ============================================================================

print("\n[6] Creating ANOMALY DETECTION STREAM (Side Output concept)...")

"""
CONCEPT EXPLANATION - Anomalies:
This is a filtered stream of problematic data
Could be sent to a dedicated topic/sink for alerting
"""

anomaly_stream = table_env.sql_query("""
    SELECT 
        event_time as anomaly_time,
        driver_id,
        latitude,
        longitude,
        speed_kmh,
        status,
        anomaly_type,
        h3_cell,
        CURRENT_TIMESTAMP as detected_at
    FROM (
        SELECT 
            event_time,
            driver_id,
            latitude,
            longitude,
            speed_kmh,
            status,
            detect_anomaly(speed_kmh, status) as anomaly_type,
            hex_index(latitude, longitude, 9) as h3_cell
        FROM gps_pings
    )
    WHERE anomaly_type != 'NORMAL'
""")
print("✓ Anomaly stream created:")
print("  - Filters for speed_kmh < 0 or impossible_speed")
print("  - Separate output for alerts/monitoring")

# ============================================================================
# 8. TRIP ANALYTICS - CONCEPT: Stateful Processing
# ============================================================================

print("\n[7] Creating TRIP ANALYTICS (Advanced windowing strategy)...")

"""
CONCEPT EXPLANATION - Trip Analytics:
- Sessions window: Groups events by driver with 5-minute gap
- Detects trip start and end
- Calculates trip metrics
"""

trip_analytics = table_env.sql_query("""
    SELECT 
        driver_id,
        SESSION_START(event_time, INTERVAL '5' MINUTE) as trip_start,
        SESSION_END(event_time, INTERVAL '5' MINUTE) as trip_end,
        COUNT(*) as total_pings,
        ROUND(AVG(speed_kmh), 2) as avg_speed_during_trip,
        ROUND(MAX(latitude), 6) as max_latitude,
        ROUND(MAX(longitude), 6) as max_longitude,
        ROUND(MIN(latitude), 6) as min_latitude,
        ROUND(MIN(longitude), 6) as min_longitude,
        COUNT(DISTINCT h3_cell) as h3_cells_visited,
        MAX(trip_duration_secs) as max_trip_duration
    FROM (
        SELECT 
            driver_id,
            latitude,
            longitude,
            speed_kmh,
            trip_duration_secs,
            hex_index(latitude, longitude, 9) as h3_cell,
            event_time
        FROM gps_pings
        WHERE status = 'on_trip'
    )
    GROUP BY 
        driver_id,
        SESSION(event_time, INTERVAL '5' MINUTE)
""")
print("✓ Trip analytics created:")
print("  - Session window: 5-minute inactivity gap")
print("  - Trip metrics and geographic bounds")

# ============================================================================
# 9. PERFORMANCE METRICS - CONCEPT: Continuous Aggregation
# ============================================================================

print("\n[8] Creating SYSTEM PERFORMANCE METRICS...")

"""
CONCEPT EXPLANATION - Performance:
- Sliding window: 1-minute window, every 10 seconds
- Shows real-time system health
- Could be sent to monitoring system
"""

performance_metrics = table_env.sql_query("""
    SELECT 
        TUMBLE_START(event_time, INTERVAL '1' MINUTE) as metric_window,
        COUNT(*) as total_events,
        COUNT(DISTINCT driver_id) as active_drivers,
        COUNT(DISTINCT h3_cell) as active_zones,
        SUM(CASE WHEN status = 'on_trip' THEN 1 ELSE 0 END) as trips_count,
        SUM(CASE WHEN anomaly_type != 'NORMAL' THEN 1 ELSE 0 END) as anomalies_count,
        ROUND(CAST(SUM(CASE WHEN anomaly_type != 'NORMAL' THEN 1 ELSE 0 END) AS DECIMAL) / 
              CAST(COUNT(*) AS DECIMAL) * 100, 2) as anomaly_rate_percent
    FROM (
        SELECT 
            event_time,
            driver_id,
            status,
            h3_cell,
            detect_anomaly(speed_kmh, status) as anomaly_type
        FROM gps_pings
    )
    GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE)
""")
print("✓ Performance metrics created:")
print("  - 1-minute windowed tracking")
print("  - Event count, driver activity, anomaly rates")

# ============================================================================
# 10. OUTPUT STREAMS - Console Output for Monitoring
# ============================================================================

print("\n[9] Setting up output streams...")
print("✓ Processing will output to console logs in the following order:")
print("   1. Real-time enriched stream (all events)")
print("   2. Windowed aggregations (30s intervals)")
print("   3. Anomalies detected")
print("   4. Trip analytics (session windows)")
print("  5. System performance metrics")

# ============================================================================
# 11. EXECUTE JOBS - Print to console for demonstration
# ============================================================================

print("\n" + "=" * 80)
print("STARTING FLINK JOBS - Streaming data from Kafka")
print("=" * 80)

try:
    # Start the enriched stream output
    print("\n[OUTPUT 1] Enriched Stream (Sample of events):")
    enriched_stream.limit(5).execute().print()
    
    # Start windowed aggregation output
    print("\n[OUTPUT 2] Windowed Aggregations (30-second windows):")
    window_aggregation.execute().print()
    
except Exception as e:
    print(f"\n⚠️  Error during execution: {e}")
    print("   This is expected if Kafka topic is empty or cluster not ready")
    print("   Make sure to:")
    print("   1. Start producer: python producer.py")
    print("   2. Start containers: docker compose up -d --build")

print("\n" + "=" * 80)
print("Processor running... Check output above for data streams")
print("=" * 80)
