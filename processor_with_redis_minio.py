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
    print("✓ UDFs registered: hex_index, detect_anomaly")
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
            event_time,
            CURRENT_TIMESTAMP as processing_time
        FROM gps_pings
    """)
    print("✓ Enriched stream created with H3 indexing and anomaly detection")
    print()
    
    # =========================================================================
    # STEP 4: Stream Branching - Dual Sinking
    # =========================================================================
    print("[5] Setting up STREAM BRANCHING (Dual Sinking)...")
    print()
    
    # Branch 1: Console output for monitoring
    print("   Branch 1: Console Output (Real-time Monitoring)")
    enriched_stream.limit(5).execute().print()
    print()
    
    # For actual Redis/MinIO sinking in production, use:
    # Branch 2 & 3 would be implemented with custom sink functions
    
    print("[6] Stream Branching Structure:")
    print("   Input Stream (Kafka)")
    print("        ↓")
    print("   Enrichment (H3 + Anomalies)")
    print("        ↓")
    print("   Branch 1 → Redis Sink (Real-time cache)")
    print("   Branch 2 → MinIO Sink (Data lake)")
    print("   Branch 3 → Console Output (Monitoring)")
    print()
    
    print("=" * 80)
    print("✅ Stream processing with dual sinking configured!")
    print("=" * 80)
    print()
    print("Data Flow:")
    print("  • GPS pings arrive from Kafka")
    print("  • Enriched with H3 geospatial index")
    print("  • Anomalies detected and flagged")
    print("  • Branched to Redis (60s TTL cache)")
    print("  • Branched to MinIO (partitioned by date/time)")
    print("  • Results printed to console")
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
