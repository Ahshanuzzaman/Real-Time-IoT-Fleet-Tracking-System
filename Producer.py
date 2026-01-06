import time
import json
import requests
from kafka import KafkaProducer
from math import radians, cos, sin, asin, sqrt

# --- Helper function for distance ---
def haversine(lon1, lat1, lon2, lat2):
    # Calculate the great circle distance between two points on the earth
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers
    return c * r

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

bus_configs = [
    {"id": "BUS-SYL", "start": "90.4125,23.8103", "end": "91.8667,24.8917", "dest": "Sylhet"},
    {"id": "BUS-COX", "start": "90.4125,23.8103", "end": "91.9731,21.4272", "dest": "Cox's Bazar"},
    {"id": "BUS-RAJ", "start": "90.4125,23.8103", "end": "88.6014,24.3745", "dest": "Rajshahi"},
    {"id": "BUS-BAR", "start": "90.4125,23.8103", "end": "90.3500,22.7010", "dest": "Barishal"},
    {"id": "BUS-DIN", "start": "90.4125,23.8103", "end": "88.6376,25.6217", "dest": "Dinajpur"},
    {"id": "BUS-KHU", "start": "90.4125,23.8103", "end": "89.5400,22.8456", "dest": "Khulna"}
]

def get_route(start, end):
    url = f"http://router.project-osrm.org/route/v1/driving/{start};{end}?overview=full&geometries=geojson"
    try:
        response = requests.get(url, timeout=10)
        return response.json()['routes'][0]['geometry']['coordinates']
    except: return []

print("📍 Loading Routes and Calculating ETA...")
bus_data = []
for config in bus_configs:
    points = get_route(config["start"], config["end"])
    bus_data.append({
        "info": config, 
        "points": points, 
        "step": 0, 
        "active": False, 
        "finished": False
    })

try:
    start_time = time.time()
    SIMULATION_SPEED_KMH = 60  # Assume bus travels at 60 km/h constant for ETA calc
    
    while True:
        elapsed = time.time() - start_time
        for i, bus in enumerate(bus_data):
            if not bus["active"] and not bus["finished"] and elapsed >= (i * 4):
                bus["active"] = True

            if bus["active"] and bus["step"] < len(bus["points"]):
                lon, lat = bus["points"][bus["step"]]
                
                # --- ETA Calculation ---
                # Remaining points
                remaining_points = bus["points"][bus["step"]:]
                dist_remaining = 0
                if len(remaining_points) > 1:
                    for j in range(len(remaining_points)-1):
                        p1 = remaining_points[j]
                        p2 = remaining_points[j+1]
                        dist_remaining += haversine(p1[0], p1[1], p2[0], p2[1])
                
                # Calculate minutes left (Distance / Speed * 60)
                eta_minutes = round((dist_remaining / SIMULATION_SPEED_KMH) * 60)

                # --- Message Payload ---
                msg = {
                    "bus_id": bus["info"]["id"],
                    "destination": bus["info"]["dest"],
                    "lat": lat,
                    "lon": lon,
                    "status": "Moving",
                    "speed": f"{SIMULATION_SPEED_KMH} km/h",
                    "eta": f"{eta_minutes} mins"
                }
                
                producer.send('location_data', value=msg)
                
                # Progress the bus (Skip points to simulate movement speed)
                bus["step"] += 12 
                
                # Arrival Logic
                if bus["step"] >= len(bus["points"]):
                    bus["active"] = False
                    bus["finished"] = True
                    arrival_msg = {
                        "bus_id": bus["info"]["id"], 
                        "destination": bus["info"]["dest"], 
                        "status": "Arrived",
                        "speed": "0 km/h",
                        "eta": "0 mins"
                    }
                    producer.send('location_data', value=arrival_msg)
                    print(f"✅ {bus['info']['id']} arrived at {bus['info']['dest']}!")

        producer.flush()
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopped.")
