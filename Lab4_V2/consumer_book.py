from pathlib import Path
from uuid import uuid4

from confluent_kafka import Consumer, KafkaError, TopicPartition

# Use a new group to read the book from the beginning on each run.
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": f"book-cleaner-{uuid4()}",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

# The topic has one partition. Record its current end offset.
partition = TopicPartition("book_lines", 0)
start_offset, end_offset = consumer.get_watermark_offsets(
    partition, timeout=10
)

# Start reading at the earliest available offset.
consumer.assign([
    TopicPartition("book_lines", 0, start_offset)
])

stopwords = {
    "a", "an", "and", "are", "as", "at", "be", "been", "but",
    "by", "for", "from", "had", "has", "have", "he", "her",
    "him", "his", "i", "in", "is", "it", "its", "me", "my",
    "of", "on", "or", "she", "that", "the", "their", "them",
    "they", "this", "to", "was", "we", "were", "with", "you",
    "your",
}


def clean_line(text):
    """Lowercase, remove punctuation, and filter stop words."""
    text = "".join(
        character if character.isalpha() else " "
        for character in text.casefold()
    )
    words = [
        word for word in text.split()
        if word not in stopwords
    ]
    return " ".join(words)


output_path = Path(__file__).parent / "cleaned_book.txt"
received = 0
written = 0
empty_polls = 0
next_offset = start_offset

try:
    with output_path.open("w", encoding="utf-8") as output:
        # Stop after reading all messages present when this script started.
        while next_offset < end_offset:
            message = consumer.poll(1.0)

            if message is None:
                empty_polls += 1
                if empty_polls >= 30:
                    raise TimeoutError(
                        "No messages received for 30 seconds."
                    )
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(message.error())

            empty_polls = 0
            text = message.value().decode("utf-8")
            cleaned = clean_line(text)

            # Skip lines that contain no words after cleaning.
            if cleaned:
                output.write(cleaned + "\n")
                written += 1

            received += 1
            next_offset = message.offset() + 1

            if received % 1000 == 0:
                print(f"Processed {received} messages.")

finally:
    # Release the consumer's resources even if an error occurs.
    consumer.close()

print(f"Read {received} messages.")
print(f"Wrote {written} cleaned lines to {output_path.name}.")