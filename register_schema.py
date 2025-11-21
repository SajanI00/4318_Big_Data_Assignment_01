# register_schema.py
import json
import requests
import sys
from pathlib import Path

SR_URL = "http://localhost:8081"
TOPIC = "orders"
SUBJECT = f"{TOPIC}-value"   # conventional subject name

def main():
    schema_path = Path(__file__).parent.parent / "schemas" / "order.avsc"
    if not schema_path.exists():
        print("Schema file not found:", schema_path)
        sys.exit(1)

    with open(schema_path, "r") as f:
        schema_json = json.load(f)

    payload = {"schema": json.dumps(schema_json)}
    resp = requests.post(f"{SR_URL}/subjects/{SUBJECT}/versions", json=payload)
    if resp.status_code in (200, 201):
        print("Registered schema. Response:", resp.json())
    else:
        print("Failed to register schema:", resp.status_code, resp.text)
        resp.raise_for_status()

if __name__ == "__main__":
    main()
