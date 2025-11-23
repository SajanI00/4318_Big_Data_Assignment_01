from fastavro import schemaless_writer, schemaless_reader
import json, io

schema = json.load(open("avro/order.avsc"))

def avro_serialize(record):
    buffer = io.BytesIO()
    schemaless_writer(buffer, schema, record)
    return buffer.getvalue()

def avro_deserialize(bytes_data):
    buffer = io.BytesIO(bytes_data)
    return schemaless_reader(buffer, schema)
