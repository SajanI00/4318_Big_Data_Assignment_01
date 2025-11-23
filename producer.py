from confluent_kafka import Producer
import uuid, random, time
from utils import avro_serialize

TOPIC = "orders"
DLQ_TOPIC = "orders_dlq"

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def generate_order():
    return {
        "orderId": str(uuid.uuid4()),
        "product": random.choice(["Item1", "Item2", "Item3"]),
        "price": round(random.uniform(10, 500), 2)
    }

def send_with_retry(record, retries=3):
    data = avro_serialize(record)

    for attempt in range(1, retries + 1):
        try:
            producer.produce(TOPIC, value=data)
            producer.flush()
            print(f"✓ Sent: {record}")
            return
        except Exception as e:
            print(f"⚠ Retry {attempt}/{retries}: {e}")
            time.sleep(1)

    # Send to DLQ
    print("🚨 Sending to DLQ:", record)
    producer.produce(DLQ_TOPIC, value=data)
    producer.flush()

if __name__ == "__main__":
    print("🔵 Producer running...")
    while True:
        order = generate_order()
        send_with_retry(order)
        time.sleep(2)
