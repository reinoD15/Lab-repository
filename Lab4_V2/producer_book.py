from pathlib import Path

from confluent_kafka import Producer

# Connect to the local Kafka broker.
producer = Producer({"bootstrap.servers": "localhost:9092"})

# Read the book stored next to this script.
book_path = Path(__file__).parent / "book.txt"
delivery_errors = []


def delivery_report(error, message):
    """Record message delivery failures."""
    if error is not None:
        delivery_errors.append(str(error))


line_count = 0

# Send each line of Pride and Prejudice as a separate message.
with book_path.open(encoding="utf-8") as book:
    for line in book:
        while True:
            try:
                producer.produce(
                    topic="book_lines",
                    value=line.rstrip("\r\n").encode("utf-8"),
                    callback=delivery_report,
                )
                break
            except BufferError:
                # Wait for space in the producer's local queue.
                producer.poll(0.1)

        # Process delivery notifications.
        producer.poll(0)
        line_count += 1

# Wait for all queued messages to be delivered.
producer.flush()

if delivery_errors:
    raise RuntimeError(f"Delivery failed: {delivery_errors[0]}")

print(f"Successfully sent {line_count} lines to book_lines.")