import json
import time
import random
import threading
from datetime import datetime
from confluent_kafka import Producer
import h3
import math

# --- CONFIGURATION ---
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:19092', # External port from our Docker Compose
    'client.id': 'uber-fleet-simulator'
}

TOPIC_NAME = "gps_pings"
# --- Delhi coordinates ---
CENTER_LAT = 28.6139
CENTER_LON = 77.2090
NUM_DRIVERS = 20  # You can increase this for a bigger city like Delhi
H3_RESOLUTION = 9  # Higher resolution means smaller hexagons, adjust based on your needs

# --- HELPER FUNCTIONS ---
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in km."""
    R = 6371
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return R * c

class DriverState:
    """Track individual driver state for realistic trip simulation (STATEFUL PROCESSING concept)."""
    def __init__(self, driver_id):
        self.driver_id = driver_id
        self.lat = CENTER_LAT + random.uniform(-0.05, 0.05)
        self.lon = CENTER_LON + random.uniform(-0.05, 0.05)
        self.prev_lat = self.lat
        self.prev_lon = self.lon
        self.status = "available"
        self.trip_start_time = None
        self.last_timestamp = time.time()

def simulate_driver(driver_state, producer):
    """Simulates a driver with stateful trip tracking and realistic behavior."""
    while True:
        current_time = time.time()
        time_delta = current_time - driver_state.last_timestamp
        driver_state.last_timestamp = current_time
        
        # STATE MACHINE: available -> on_trip -> available
        # This demonstrates STATEFUL PROCESSING concept
        if driver_state.status == "available" and random.random() < 0.1:  # 10% chance to start trip
            driver_state.status = "on_trip"
            driver_state.trip_start_time = current_time
            # Move more aggressively when on trip
            driver_state.lat += random.uniform(-0.01, 0.01)
            driver_state.lon += random.uniform(-0.01, 0.01)
        elif driver_state.status == "on_trip" and (current_time - driver_state.trip_start_time) > random.uniform(30, 120):
            driver_state.status = "available"
            driver_state.trip_start_time = None
        elif driver_state.status == "on_trip":
            # Move actively during trip
            driver_state.lat += random.uniform(-0.005, 0.005)
            driver_state.lon += random.uniform(-0.005, 0.005)
        else:
            # Small idle movement when available
            driver_state.lat += random.uniform(-0.0005, 0.0005)
            driver_state.lon += random.uniform(-0.0005, 0.0005)
        
        # Calculate speed (km/h) - enables performance anomaly detection in processor
        distance_km = haversine_distance(driver_state.prev_lat, driver_state.prev_lon, 
                                        driver_state.lat, driver_state.lon)
        speed_kmh = (distance_km / time_delta) * 3600 if time_delta > 0 else 0
        
        # Create enriched payload with extended data
        payload = {
            "driver_id": f"DRV_{driver_state.driver_id}",
            "latitude": round(driver_state.lat, 6),
            "longitude": round(driver_state.lon, 6),
            "status": driver_state.status,
            "speed_kmh": round(speed_kmh, 2),
            "timestamp": datetime.utcnow().isoformat(),
            "trip_duration_secs": int(current_time - driver_state.trip_start_time) if driver_state.trip_start_time else 0
        }
        
        # Produce to Redpanda
        producer.produce(
            TOPIC_NAME, 
            key=payload["driver_id"], 
            value=json.dumps(payload), 
            callback=delivery_report
        )
        producer.flush()
        
        driver_state.prev_lat = driver_state.lat
        driver_state.prev_lon = driver_state.lon
        time.sleep(random.uniform(2, 5))  # More realistic 2-5 second intervals

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    p = Producer(KAFKA_CONFIG)
    print(f"🚀 Starting simulation for {NUM_DRIVERS} drivers...")
    
    threads = []
    for i in range(NUM_DRIVERS):
        driver_state = DriverState(i)  # Create individual state for each driver
        t = threading.Thread(target=simulate_driver, args=(driver_state, p))
        t.daemon = True
        t.start()
        threads.append(t)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 Simulation stopped.")
    except KeyboardInterrupt:
        print("🛑 Simulation stopped.")
