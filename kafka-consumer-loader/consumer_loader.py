import os
import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime

# --- Environment Configuration ---
# The container sees other services by their names (kafka, mongodb)
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092') 
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'site_sensors') # Ensure this matches your Topic name!
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://mongodb:27017/') 
MONGO_DB = "crane_data"
MONGO_COLLECTION = "raw_telemetry"

# --- Connections ---

def connect_to_mongo():
    """Attempts to connect to MongoDB and returns the collection."""
    client = MongoClient(MONGO_URI)
    # Ping the database to ensure connection readiness
    try:
        client.admin.command('ping')
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        print(f"Connected to MongoDB. Database: {MONGO_DB}, Collection: {MONGO_COLLECTION}")
        return collection
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        # Add retry logic here if necessary
        raise

def create_kafka_consumer():
    """Creates the Kafka Consumer."""
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest', # Start reading at the earliest offset if no committed offset is found
        enable_auto_commit=True,
        group_id='mongo-loader-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Deserializes byte messages to JSON
    )
    print(f"Kafka Consumer started for topic: {KAFKA_TOPIC}, Broker: {KAFKA_BROKER}")
    return consumer

# --- Data Streaming Logic ---

def run_pipeline():
    """The main loop: reads from Kafka and writes to MongoDB."""
    mongo_collection = connect_to_mongo()
    kafka_consumer = create_kafka_consumer()
    
    message_count = 0
    print("Starting to listen for messages...")
    
    for message in kafka_consumer:
        # message.value is already a dictionary due to the deserializer
        data = message.value
        
        # Add an ingestion timestamp (Data Lake best practice)
        data['ingestion_time'] = datetime.now().isoformat()
        
        try:
            mongo_collection.insert_one(data)
            message_count += 1
            
            # Print a progress update
            if message_count % 100 == 0:
                print(f"Loaded {message_count} messages. Last sensor: {data.get('sensor_id')} at {data.get('timestamp')}")

        except Exception as e:
            print(f"Error loading message to Mongo: {e}. Data: {data}")
            
if __name__ == "__main__":
    run_pipeline()
