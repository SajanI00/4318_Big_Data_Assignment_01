# consumer.py
import time
import struct
import json
import requests
from confluent_kafka import Consumer, Producer
from fastavro import schemaless_reader
from io import BytesIO

KAFKA_BOOTSTRAP = "localhost:9092"
SR_URL = "http://localhost:8081"
TOPIC = "orders"
DLQ_TOPIC = "orders.dlq"
GROUP_ID = "orders-processor"

# Consumer config
cons_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}
consumer = Consumer(cons_conf)
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

def get_schema_by_id(schema_id):
    r = requests.get(f"{SR_URL}/schemas/ids/{schema_id}")
    r.raise_for_status()
    return json.loads(r.text)

def parse_avro_message(msg_bytes):
    if not msg_bytes or len(msg_bytes) < 5:
        raise ValueError("Invalid payload")
    magic = msg_bytes[0]
    if magic != 0:
        raise ValueError("Unsupported wire format")
    schema_id = struct.unpack('>I', msg_bytes[1:5])[0]
    avro_payload = msg_bytes[5:]
    schema = get_schema_by_id(schema_id)
    bio = BytesIO(avro_payload)
    record = schemaless_reader(bio, schema)
    return record

# In-memory running average
count = 0
sum_prices = 0.0

def process_record(record):
    """
    Business logic:
      - If price < 0 -> permanent error (send to DLQ)
      - If price < 10 -> temporary error (simulated)
      - Else -> success: update running average
    """
    global count, sum_prices

    price = record.get("price")
    if price is None:
        raise Exception("Permanent: missing price")
    if price < 0:
        # treat as permanent
        raise ValueError("Permanent: negative price")
    if price < 10.0:
        # simulate transient error
        raise RuntimeError("Temporary: external resource failure (simulated)")

    # success
    count += 1
    sum_prices += price
    avg = sum_prices / count
    print(f"SUCCESS: order={record['orderId']} price={price:.2f} running_avg={avg:.2f}")

def send_to_dlq(original_bytes, reason, headers=None):
    # Attach reason in header for convenience
    hdrs = headers or []
    hdrs.append(("dlq-reason", reason))
    producer.produce(DLQ_TOPIC, value=original_bytes, headers=hdrs)
    producer.flush()
    print(f"Sent message to DLQ. Reason: {reason}")

def consumer_loop():
    consumer.subscribe([TOPIC])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error:", msg.error())
                continue
            payload = msg.value()
            # parse
            try:
                record = parse_avro_message(payload)
            except Exception as e:
                print("Parse error -> DLQ:", e)
                send_to_dlq(payload, f"parse-error: {e}")
                consumer.commit(message=msg)
                continue

            # in-process retry
            max_attempts = 3
            attempt = 0
            while attempt < max_attempts:
                try:
                    process_record(record)
                    # success -> commit offset
                    consumer.commit(message=msg)
                    break
                except RuntimeError as ex:
                    # temporary error -> retry with exponential backoff
                    attempt += 1
                    print(f"Temporary error on attempt {attempt}: {ex}")
                    time.sleep(2 ** attempt)  # 2,4,8 seconds
                    continue
                except ValueError as ex:
                    # permanent error -> DLQ
                    print("Permanent error:", ex)
                    send_to_dlq(payload, str(ex))
                    consumer.commit(message=msg)
                    break
                except Exception as ex:
                    # other unexpected -> treat as permanent
                    print("Unexpected error (permanent):", ex)
                    send_to_dlq(payload, f"unexpected:{ex}")
                    consumer.commit(message=msg)
                    break
            else:
                # retries exhausted
                print("Retries exhausted -> DLQ")
                send_to_dlq(payload, "retries-exhausted")
                consumer.commit(message=msg)
    except KeyboardInterrupt:
        print("Consumer interrupted")
    finally:
        consumer.close()

if __name__ == "__main__":
    consumer_loop()
