import time
import json
import random
from kafka import KafkaProducer

KAFKA_BROKER = '192.168.1.30:9092' 
KAFKA_TOPIC = 'site_sensors'

def generate_sensor_data():
    """Generates simulated data for a construction site sensor."""
    data = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "sensor_id": f"CRANE_{random.choice(['A', 'B', 'C'])}",
        "load_kg": round(random.uniform(500.0, 4500.0), 2),  # LOAD
        "noise_db": random.randint(70, 110),                 # NOISE
        "temperature_c": round(random.uniform(20.0, 45.0), 1), # TEMPERTURE
    }
    return data

if __name__ == "__main__":
    try:
        # Producer. value_serialize -JSON (utf-8)
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"Producer started, targeting {KAFKA_BROKER}")

        while True:
            sensor_reading = generate_sensor_data()
            print(f"Sending: {sensor_reading}")
            
            # send to-Topic
            future = producer.send(KAFKA_TOPIC, value=sensor_reading)
            producer.flush() 
            
            time.sleep(1) # send data each second

    except Exception as e:
        print(f"An error occurred: {e}")
        print("Connection failed. Check network, IP address, and Kafka status on Ubuntu.")
