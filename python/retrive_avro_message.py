from kafka import KafkaConsumer
import json
import avro.io
import avro.schema
import io
import sys

SCHEMA_PATH='./resources/user.avsc'
TOPIC_NAME='avro_messages'

consumer = KafkaConsumer( 
    bootstrap_servers=['localhost'],
    enable_auto_commit=True,
    group_id='test01'
  )

schema_path = SCHEMA_PATH
schema = avro.schema.Parse(open(schema_path).read())
reader = avro.io.DatumReader(schema)

consumer.subscribe([TOPIC_NAME])
for message in consumer:
    bytes_reader = io.BytesIO(message.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    try:
        decoded_msg = reader.read(decoder)
        print(decoded_msg)
        sys.stdout.flush()
    except AssertionError:
        continue
