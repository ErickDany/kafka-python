from kafka.admin import KafkaAdminClient, NewTopic

TOPIC_NAME='avro_messages'

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092", 
    client_id='test'
)

#list_topics=admin_client.list_topics()
topic_list = []
try:
    topic_list.append(NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print(f'Topic \'{TOPIC_NAME}\' was created')
except:
    print(f'Topic \'{TOPIC_NAME}\' already existis')