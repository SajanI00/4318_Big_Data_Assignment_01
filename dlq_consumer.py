from confluent_kafka import Consumer
from utils import avro_deserialize

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "dlq-consumer-group",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["orders_dlq"])

print("🚨 DLQ Consumer running...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue

    if msg.error():
        print("Error:", msg.error())
        continue

    order = avro_deserialize(msg.value())
    print("❗ DLQ Order:", order)
