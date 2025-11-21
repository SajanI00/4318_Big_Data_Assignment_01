# producer.py
import time
import uuid
import json
import requests
import random
from confluent_kafka import Producer
from fastavro import schemaless_writer
from io import BytesIO
import struct
from pathlib import Path

KAFKA_BOOTSTRAP = "localhost:9092"
SR_URL = "http://localhost:8081"
TOPIC = "orders"

def get_latest_schema_id(topic=TOPIC):
    subject = f"{topic}-value"
    r = requests.get(f"{SR_URL}/subjects/{subject}/versions")
    r.raise_for_status()
    versions = r.json()
    latest = versions[-1]
    r2 = requests.get(f"{SR_URL}/subjects/{subject}/versions/{latest}")
    r2.raise_for_status()
    return r2.json()['id'], r2.json()['schema']

def avro_serialize(schema_id, schema_dict, record):
    bio = BytesIO()
    schemaless_writer(bio, schema_dict, record)
    avro_bytes = bio.getvalue()
    return b'\x00' + struct.pack('>I', schema_id) + avro_bytes

def make_order():
    return {
        "orderId": str(uuid.uuid4())[:8],
        "product": random.choice(["ItemA", "ItemB", "ItemC"]),
        "price": round(random.uniform(1.0, 200.0), 2)
    }

def main():
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    schema_id, schema_str = get_latest_schema_id()
    schema = json.loads(schema_str)
    print("Using schema id:", schema_id)

    try:
        for i in range(50):
            order = make_order()
            # optionally inject some problematic records for demo:
            # Make a few messages with price < 0 to simulate permanent error
            if i % 20 == 0:
                order['price'] = -1.0   # will be treated as permanent error in consumer demo
            # Make some messages with small price to simulate temporary failure
            if i % 7 == 0 and order['price'] >= 0:
                order['price'] = round(random.uniform(0.1, 9.9), 2)

            payload = avro_serialize(schema_id, schema, order)
            p.produce(TOPIC, value=payload)
            p.poll(0)
            print("Produced:", order)
            time.sleep(0.3)
    finally:
        p.flush()

if __name__ == "__main__":
    main()
