from kafka import KafkaProducer
import avro.io
import avro.schema
import io
from faker import Faker
import random
import time

SCHEMA_PATH='./resources/user.avsc'
TOPIC_NAME='avro_messages'

fake=Faker()
colors=['Greys', 'Purples', 'Blues', 'Greens', 'Oranges', 'Reds','YlOrBr', 'YlOrRd', 'OrRd', 'PuRd', 'RdPu', 'BuPu','GnBu', 'PuBu', 'YlGnBu', 'PuBuGn', 'BuGn', 'YlGn']
numbers=list(range(1,101))
key_ids = list(range(1,5))

schema_path = SCHEMA_PATH
schema = avro.schema.Parse(open(schema_path).read())

producer=KafkaProducer(bootstrap_servers=['localhost'],
                       key_serializer=str.encode)

while True:
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer = avro.io.DatumWriter(schema)
    writer.write(
                    {
                        "name": fake.name(),
                        "favorite_color": random.choice(colors),
                        "favorite_number": random.choice(numbers)
                    }, encoder)
    raw_bytes = bytes_writer.getvalue()
    key=f'key{random.choice(key_ids)}'
    producer.send(topic=TOPIC_NAME,
                  key=key,
                  value=raw_bytes)
    time.sleep(10)