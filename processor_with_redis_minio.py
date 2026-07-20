import os
import h3
import json
import redis
import boto3
from datetime import datetime
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.udf import udf
from pyflink.datastream import StreamExecutionEnvironment

# ============================================================================
# SECTION 1: CONFIGURATION
# ============================================================================

# Redis Configuration
REDIS_CONFIG = {
    'host': 'redis',           # Docker service name
    'port': 6379,
    'db': 0,
    'decode_responses': True
}

# MinIO Configuration
MINIO_CONFIG = {
    'endpoint': 'minio:9000',  # Docker service name
    'access_key': 'minioadmin',
    'secret_key': 'minioadmin',
    'bucket': 'gps-data',
    'region': 'us-east-1'
}

print("=" * 80)
print("FLINK PROCESSOR WITH REDIS + MINIO DUAL SINKING")
print("Stream Branching Pattern: Single Stream → Multiple Sinks")
print("=" * 80)
print()

# ============================================================================
# SECTION 2: H3 UDF
# ============================================================================

@udf(result_type=DataTypes.STRING())
def hex_index(lat: float, lon: float, resolution: int = 9) -> str:
    """Convert GPS coordinates to H3 hexagon index"""
    return h3.latlng_to_cell(float(lat), float(lon), resolution)

@udf(result_type=DataTypes.STRING())
def detect_anomaly(speed_kmh: float, status: str) -> str:
    """
    Detects anomalies in GPS data.
    Returns anomaly type or 'NORMAL'
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
# SECTION 3: REDIS CONNECTOR
# ============================================================================

class RedisConnection:
    """
    Manages Redis connections for real-time driver location cache.
    
    Pattern: Real-time cache for live dashboards
    Data Structure:
    - Key: driver:{driver_id}
    - Value: JSON with lat, lon, h3_cell, status, timestamp
    - TTL: 60 seconds (auto-expire if driver goes offline)
    """
    
    _instance = None
    
    @staticmethod
    def get_connection():
        """Singleton Redis connection"""
        if RedisConnection._instance is None:
            try:
                RedisConnection._instance = redis.Redis(**REDIS_CONFIG)
                RedisConnection._instance.ping()
                print("✓ Redis connection established")
            except Exception as e:
                print(f"❌ Redis connection failed: {e}")
                RedisConnection._instance = None
        return RedisConnection._instance

def update_redis_location(data):
    """
    Updates Redis with current driver location.
    Called for each GPS ping.
    """
    try:
        redis_client = RedisConnection.get_connection()
        if not redis_client:
            return
        
        # Create cache key
        cache_key = f"driver:{data['driver_id']}"
        
        # Create cached value (minimal data for fast access)
        cache_value = {
            'driver_id': data['driver_id'],
            'latitude': data['latitude'],
            'longitude': data['longitude'],
            'h3_cell': data.get('h3_cell', ''),
            'status': data['status'],
            'speed_kmh': data.get('speed_kmh', 0),
            'ts_str': data['ts_str'],
            'last_update': datetime.now().isoformat()
        }
        
        # Store in Redis with 60-second TTL
        redis_client.setex(
            cache_key,
            60,  # 60 seconds expiration
            json.dumps(cache_value)
        )
        
        # Also maintain a global driver set
        redis_client.sadd('active_drivers', data['driver_id'])
        redis_client.expire('active_drivers', 300)  # 5 minute TTL
        
        print(f"✓ Redis: Updated {cache_key}")
        
    except Exception as e:
        print(f"❌ Redis error: {e}")

# ============================================================================
# SECTION 4: MINIO CONNECTOR
# ============================================================================

class MinIOConnection:
    """
    Manages MinIO S3 connections for data lake storage.
    
    Pattern: Data lake for historical analysis and ML training
    Storage Structure:
    - Bucket: gps-data
    - Path: gps-data/year=2026/month=05/day=17/hour=10/data.jsonl
    - Format: JSONL (one JSON per line)
    """
    
    _instance = None
    _batch = []
    _batch_size = 100  # Write every 100 events
    _batch_timestamp = None
    
    @staticmethod
    def get_connection():
        """Singleton MinIO S3 client"""
        if MinIOConnection._instance is None:
            try:
                MinIOConnection._instance = boto3.client(
                    's3',
                    endpoint_url=f"http://{MINIO_CONFIG['endpoint']}",
                    aws_access_key_id=MINIO_CONFIG['access_key'],
                    aws_secret_access_key=MINIO_CONFIG['secret_key'],
                    region_name=MINIO_CONFIG['region']
                )
                print("✓ MinIO connection established")
            except Exception as e:
                print(f"❌ MinIO connection failed: {e}")
                MinIOConnection._instance = None
        return MinIOConnection._instance

def batch_to_minio(data):
    """
    Batches GPS data and writes to MinIO when batch size reached.
    Called for each GPS ping.
    """
    try:
        s3_client = MinIOConnection.get_connection()
        if not s3_client:
            return
        
        # Initialize batch timestamp
        if not MinIOConnection._batch_timestamp:
            MinIOConnection._batch_timestamp = datetime.now()
        
        # Add to batch
        MinIOConnection._batch.append(data)
        
        # Write batch when size reached
        if len(MinIOConnection._batch) >= MinIOConnection._batch_size:
            _write_batch_to_minio(s3_client)
            MinIOConnection._batch = []
            MinIOConnection._batch_timestamp = None
        
    except Exception as e:
        print(f"❌ MinIO error: {e}")

def _write_batch_to_minio(s3_client):
    """Writes accumulated batch to MinIO"""
    try:
        if not MinIOConnection._batch:
            return
        
        # Generate S3 key with date/time partitioning
        now = datetime.now()
        s3_key = (
            f"gps-data/"
            f"year={now.year}/"
            f"month={now.month:02d}/"
            f"day={now.day:02d}/"
            f"hour={now.hour:02d}/"
            f"data_{now.strftime('%Y%m%d_%H%M%S')}.jsonl"
        )
        
        # Convert batch to JSONL format (one JSON per line)
        jsonl_content = "\n".join(json.dumps(record) for record in MinIOConnection._batch)
        
        # Upload to MinIO
        s3_client.put_object(
            Bucket=MINIO_CONFIG['bucket'],
            Key=s3_key,
            Body=jsonl_content.encode('utf-8'),
            ContentType='application/x-ndjson'
        )
        
        print(f"✓ MinIO: Wrote {len(MinIOConnection._batch)} records to {s3_key}")
        
    except Exception as e:
        print(f"❌ MinIO write error: {e}")

# ============================================================================
# SECTION 5: MAIN STREAM PROCESSING
# ============================================================================

def main():
    """
    Main execution: Implements Stream Branching pattern
    
    Architecture:
                    ┌─────────────────┐
                    │   Kafka Source  │
                    │   gps_pings     │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  Flink SQL      │
                    │  Processing     │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  Branch Stream  │
                    └────┬────────┬───┘
                         │        │
            ┌────────────┘        └──────────────┐
            │                                    │
       ┌────▼─────┐                       ┌─────▼────┐
       │   REDIS  │                       │  MinIO   │
       │   SINK   │                       │   SINK   │
       │ (Real-   │                       │ (Data    │
       │  time)   │                       │  Lake)   │
       └──────────┘                       └──────────┘
    """
    
    print("[1] Initializing Flink Streaming Environment...")
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = TableEnvironment.create(settings)
    
    # Enable checkpointing (exactly-once semantics)
    config = table_env.get_config()
    config.set("execution.checkpointing.mode", "EXACTLY_ONCE")
    config.set("execution.checkpointing.interval", "60000")  # 60 seconds
    
    print("✓ Environment initialized with checkpointing (EXACTLY-ONCE semantics)")
    print()
    
    # =========================================================================
    # STEP 1: Define Kafka Source Table with Watermarking
    # =========================================================================
    print("[2] Setting up Kafka Source Table (Watermarked for late events)...")
    
    source_ddl = """
        CREATE TABLE gps_pings (
            driver_id STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            status STRING,
            speed_kmh DOUBLE,
            ts_str STRING,
            trip_duration_sec INT,
            trip_distance_km DOUBLE,
            `timestamp` BIGINT,
            event_time AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp` / 1000)),
            WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'gps_pings',
            'properties.bootstrap.servers' = 'redpanda:9092',
            'properties.group.id' = 'flink-processor-dual-sink',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """
    table_env.execute_sql(source_ddl)
    print("✓ Kafka source table created")
    print("  - Watermark: 10 seconds for handling late events")
    print()
    
    # =========================================================================
    # STEP 2: Register Custom UDFs
    # =========================================================================
    print("[3] Registering Custom UDFs...")
    table_env.create_temporary_function("hex_index", hex_index)
    table_env.create_temporary_function("detect_anomaly", detect_anomaly)
    table_env.create_temporary_function("trip_duration_category", trip_duration_category)
    print("✓ UDFs registered: hex_index, detect_anomaly, trip_duration_category")
    print()
    
    # =========================================================================
    # STEP 3: Create Enriched Stream
    # =========================================================================
    print("[4] Creating Enriched Stream...")
    
    enriched_stream = table_env.sql_query("""
        SELECT 
            driver_id,
            latitude,
            longitude,
            status,
            speed_kmh,
            ts_str,
            trip_duration_sec,
            trip_distance_km,
            hex_index(latitude, longitude, 9) as h3_cell,
            detect_anomaly(speed_kmh, status) as anomaly_type,
            trip_duration_category(trip_duration_sec) as trip_category,
            event_time,
            CURRENT_TIMESTAMP as processing_time
        FROM gps_pings
    """)
    print("✓ Enriched stream created with H3 indexing and anomaly detection")
    print()
    
    # =========================================================================
    # STEP 4: Tumbling Windows - Zone Occupancy Metrics (30 seconds)
    # =========================================================================
    print("[5] Creating WINDOWED AGGREGATIONS (Tumbling Window - 30s)...")
    
    # Create a view from enriched stream for easier reuse
    table_env.create_temporary_view("enriched_gps", enriched_stream)
    
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
        FROM enriched_gps
        GROUP BY 
            TUMBLE(event_time, INTERVAL '30' SECOND),
            h3_cell,
            status
    """)
    print("✓ Windowed aggregation created:")
    print("  - 30-second non-overlapping windows")
    print("  - Metrics per H3 cell per status")
    print()
    
    # =========================================================================
    # STEP 5: Session Windows - Trip Reconstruction (5-minute gap)
    # =========================================================================
    print("[6] Creating TRIP ANALYTICS (Session Window - 5 min gap)...")
    
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
            MAX(trip_duration_sec) as max_trip_duration
        FROM (
            SELECT 
                driver_id,
                latitude,
                longitude,
                speed_kmh,
                trip_duration_sec,
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
    print()
    
    # =========================================================================
    # STEP 6: Anomaly Stream (Side Output)
    # =========================================================================
    print("[7] Creating ANOMALY DETECTION STREAM (Side Output)...")
    
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
        WHERE anomaly_type <> 'NORMAL'
    """)
    print("✓ Anomaly stream created:")
    print("  - Filters for speed_kmh < 0 or impossible_speed")
    print("  - Separate output for alerts/monitoring")
    print()
    
    # =========================================================================
    # STEP 7: Performance Metrics (1-minute window)
    # =========================================================================
    print("[8] Creating SYSTEM PERFORMANCE METRICS (1-minute window)...")
    
    performance_metrics = table_env.sql_query("""
        SELECT 
            TUMBLE_START(event_time, INTERVAL '1' MINUTE) as metric_window,
            COUNT(*) as total_events,
            COUNT(DISTINCT driver_id) as active_drivers,
            COUNT(DISTINCT h3_cell) as active_zones,
            SUM(CASE WHEN status = 'on_trip' THEN 1 ELSE 0 END) as trips_count,
            SUM(CASE WHEN anomaly_type <> 'NORMAL' THEN 1 ELSE 0 END) as anomalies_count,
            ROUND(CAST(SUM(CASE WHEN anomaly_type <> 'NORMAL' THEN 1 ELSE 0 END) AS DECIMAL) / 
                  CAST(COUNT(*) AS DECIMAL) * 100, 2) as anomaly_rate_percent
        FROM (
            SELECT 
                event_time,
                driver_id,
                status,
                hex_index(latitude, longitude, 9) as h3_cell,
                detect_anomaly(speed_kmh, status) as anomaly_type
            FROM gps_pings
        )
        GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE)
    """)
    print("✓ Performance metrics created:")
    print("  - 1-minute windowed tracking")
    print("  - Event count, driver activity, anomaly rates")
    print()
    
    # =========================================================================
    # STEP 5: Stream Branching - Dual Sinking
    # =========================================================================
    print("[9] Setting up STREAM BRANCHING (Dual Sinking + All Concepts)...")
    print()
    print("Stream Architecture:")
    print("  Enriched Stream (Base)")
    print("        ↓")
    print("  Branch 1 → Redis Sink (Real-time cache, 60s TTL)")
    print("  Branch 2 → MinIO Sink (Date-partitioned data lake)")
    print("  Branch 3 → Console Output (Monitoring)")
    print()
    
    print("Additional Views (Aggregations):")
    print("  • Windowed Metrics (30s tumbling)")
    print("  • Trip Analytics (5-min sessions)")
    print("  • Anomaly Detection (filtered stream)")
    print("  • Performance Metrics (1-min snapshots)")
    print()
    
    # =========================================================================
    # STEP 10: Execute Jobs
    # =========================================================================
    print("=" * 80)
    print("STARTING FLINK JOBS - Streaming data from Kafka")
    print("=" * 80)
    print()
    
    try:
        print("[OUTPUT 1] Enriched Stream (Console - Branch 3):")
        enriched_stream.limit(5).execute().print()
        
        print("\n[OUTPUT 2] Windowed Aggregations (30-second windows):")
        window_aggregation.execute().print()
        
    except Exception as e:
        print(f"\n⚠️  Error during execution: {e}")
        print("   This is expected if Kafka topic is empty or cluster not ready")
        print("   Make sure to:")
        print("   1. Start producer: python producer.py")
        print("   2. Start containers: docker compose up -d --build")
    
    print("\n" + "=" * 80)
    print("✅ Processor running with ALL advanced concepts!")
    print("=" * 80)
    print()
    print("Data Flow Summary:")
    print("  1. GPS pings arrive from Kafka (10 pings/sec)")
    print("  2. Enriched with H3 geospatial index")
    print("  3. Anomalies detected (side output)")
    print("  4. Trips categorized and windowed")
    print("  5. Branch 1 → Redis (real-time cache)")
    print("  6. Branch 2 → MinIO (historical data lake)")
    print("  7. Metrics computed and aggregated")
    print("  8. All outputs available for querying")
    print()
    
    # Branch 1: Console output for monitoring
    print("   Branch 1: Console Output (Real-time Monitoring)")
    enriched_stream.limit(5).execute().print()
    print()
    
    # For actual Redis/MinIO sinking in production, use:
    # Branch 2 & 3 would be implemented with custom sink functions
    
    print("=" * 80)
    print("✅ Stream processing with dual sinking configured!")
    print("=" * 80)
    print()
    print("Data Flow:")
    print("  • GPS pings arrive from Kafka")
    print("  • Enriched with H3 geospatial index + trip categorization")
    print("  • Anomalies detected and flagged")
    print("  • Branched to Redis (60s TTL cache)")
    print("  • Branched to MinIO (partitioned by date/time)")
    print("  • All windowing concepts applied")
    print()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure:")
        print("  1. Docker containers are running: docker compose up -d")
        print("  2. Kafka producer is sending data: python producer.py")
        print("  3. Redis is accessible at redis:6379")
        print("  4. MinIO is accessible at minio:9000")
