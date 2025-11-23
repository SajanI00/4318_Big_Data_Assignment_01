from confluent_kafka import Consumer
from utils import avro_deserialize
import random

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-consumer-group",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["orders"])

total_price = 0
count = 0

def process_order(order):
    global total_price, count

    # simulate random temporary failure
    if random.random() < 0.1:
        raise Exception("Simulated temporary failure")

    total_price += order["price"]
    count += 1
    avg = total_price / count

    print(f"✓ Order: {order} | Running Avg = {avg:.2f}")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue

    if msg.error():
        print("Consumer error:", msg.error())
        continue

    try:
        order = avro_deserialize(msg.value())
        process_order(order)
    except Exception as e:
        print("⚠ Failed to process:", e)
