from confluent_kafka.admin import AdminClient, NewTopic

# Connect to the local Kafka broker.
admin = AdminClient({"bootstrap.servers": "localhost:9092"})

# Create a topic for the lines of the book.
topic = NewTopic("book_lines", num_partitions=1, replication_factor=1)
results = admin.create_topics([topic])

# Wait for Kafka to confirm the topic creation.
for name, future in results.items():
    future.result()
    print(f"Topic created: {name}")