from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
import random
import string
import json
from datetime import datetime
import time

TOPIC_NAME='example_topic'

user_ids = list(range(1,101))
recipient_ids=list(range(1,101))
key_ids = list(range(1,5))

def generate_message()->dict:
    random_user_id=random.choice(user_ids)
    recipient_ids_copy=recipient_ids.copy()
    recipient_ids_copy.remove(random_user_id)
    random_recipient_id=random.choice(recipient_ids_copy)
    message=''.join(random.choice(string.ascii_letters) for i in range(32))
    return {
        'user_id': random_user_id,
        'recipient_id':random_recipient_id,
        'message':message
    }

def serializer(message):
    return json.dumps(message).encode('utf-8')

producer=KafkaProducer(bootstrap_servers=['localhost'],
                       key_serializer=str.encode,
                       value_serializer=serializer)

while True:
    dummy_message=generate_message()
    print(f'Producing message @ {datetime.now()} | Message = {str(dummy_message)}')
    producer.send(topic=TOPIC_NAME,
                  key=f'key{random.choice(key_ids)}',
                  value=dummy_message)
    time.sleep(10)

print(generate_data())


