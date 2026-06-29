from confluent_kafka.admin import AdminClient, NewTopic
import time

admin_client = AdminClient({
    'bootstrap.servers': 'localhost:9092'
})
topics = [
    NewTopic(
        topic='fleet-telemetry-raw',
        num_partitions=10,
        replication_factor=1,
        config={
            'retention.ms': str(24*60*1000),
            'cleanup.policy': 'delete'
        }
    ),
    NewTopic(
        topic='fleet-anomaly-alerts',
        num_partitions=5,
        replication_factor=1,
        config={
            'retention.ms': str(7*24*60*60*1000),
            'cleanup.policy': 'delete'
        }
    )
]

print('Creating topics...')
futures = admin_client.create_topics(topics)

for topic_name, future in futures.items():
    try:
        future.result()
        print(f' topics "{topic_name}" created successfully')
    except Exception as e:
        if 'TOPIC_ALREADY_EXISTS' in str(e):
            print(f' Topic "{topic_name}" already exists (skipping)')
        else:
            print(f' Failed to create "{topic_name}" {e}')

time.sleep(1)

metadata = admin_client.list_topics(timeout=10)
print(f'\nTopics in cluster:')
for topic_name in sorted(metadata.topics.keys()):
    if not topic_name.startswith('_'):
        topic_info = metadata.topics[topic_name]
        print(f' {topic_name} ({len(topic_info.partitions)} partitions)')
        
print(']nSetup complete. ready to run simulator.')