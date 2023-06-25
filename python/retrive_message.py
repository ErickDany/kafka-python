from kafka import KafkaConsumer
import json


TOPIC_NAME='example_topic'

consumer = KafkaConsumer( 
    bootstrap_servers=['localhost'],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id='test01',
    key_deserializer=lambda key: key.decode('utf-8'),
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
  )

consumer.subscribe([TOPIC_NAME])
for message in consumer:
    message = f"""
    Message received: {message.value}
    Message key: {message.key}
    Message partition: {message.partition}
    Message offset: {message.offset}
    """
    print(message)