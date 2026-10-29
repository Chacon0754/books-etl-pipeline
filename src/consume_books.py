import argparse
import json
import logging
import os
import time
from pathlib import Path
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from src.transform_books import transform_api_book, transform_scraped_book

from src.config import RAW_DIRECTORY, PROCESSED_DIRECTORY

logger = logging.getLogger(__name__)

BATCH_PATH = RAW_DIRECTORY / "kafka_batch.json"

OUTPUTS = {
    "books_api": ("books_api.json", transform_api_book),
    "books_scraping": ("books_scraped.json", transform_scraped_book),
}

def consume_topic(consumer: Consumer, topic: str, specification: dict, batch_id: str, transform, timeout: int) -> list:
    consumer.assign(
        [
            TopicPartition(topic, specification["partition"], specification["start_offset"])
        ]
    )

    books = []
    expected_count = specification["count"]
    deadline = time.monotonic() + timeout

    while len(books) < expected_count:
        if time.monotonic()>= deadline:
            raise TimeoutError(f"Timed out consuming {topic}: {len(books)}/{expected_count}")
        
        message =consumer.poll(1)

        if message is None:
            continue

        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                raise RuntimeError(
                    f"Reached the end of {topic} before completing "
                    f"the batch: {len(books)}/{expected_count} records."
                )
            
            raise KafkaException(message.error())
        
        if message.offset() >= specification["end_offset"]:
            raise RuntimeError(
                f"The batch offset range ended before receiving "
                f"all expected records from {topic}."
            )
        
        event = json.loads(message.value().decode("utf-8"))

        if event.get("batch_id") != batch_id:
            continue
            
        
        books.append(transform(event["book"]))
    
    if len({book["source_id"] for book in books}) != len(books):
        raise ValueError(f"Duplicate book identifiers found in {topic}.")

    logger.info(f"Consumed and transformed {len(books)} books from {topic}.")

    return books

def consume_books(timeout:int = 120) -> Path:
    if timeout < 1:
        raise ValueError("The timeout must be greater than zero.")
    
    with BATCH_PATH.open("r", encoding="utf-8") as batch_file:
        batch = json.load(batch_file)
    
    batch_id = batch["batch_id"]

    results = {}

    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": f"books-transform-{batch_id}",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "error",
            "enable.partition.eof": True,
        }
    )

    logger.info(f"Processing batch: {batch_id}")

    try:
        for topic, (_, transform) in OUTPUTS.items():
            results[topic] = consume_topic(
                consumer=consumer,
                topic=topic,
                specification=batch["topics"][topic],
                batch_id=batch_id,
                transform=transform,
                timeout=timeout
            )
    finally:
        consumer.close()
    
    PROCESSED_DIRECTORY.mkdir(parents=True, exist_ok=True)

    manifest = {
        "batch_id": batch_id,
        "collections": {}
    }

    for topic, (filename, _) in OUTPUTS.items():
        output_path = PROCESSED_DIRECTORY / filename
        books = results[topic]

        output = {
            "batch_id": batch_id,
            "processed_count": len(books),
            "docs": books,
        }

        with output_path.open("w", encoding="utf-8") as output_file:
            json.dump(output, output_file, ensure_ascii=False, indent=2)
        
        manifest["collections"][output_path.stem] = {
            "file": filename,
            "count": len(books),
        }

        logger.info(f"Processed data saved to: {output_path}")
    
    manifest_path = PROCESSED_DIRECTORY / "processed_batch.json"

    with manifest_path.open("w", encoding="utf-8") as manifest_file:
        json.dump(manifest, manifest_file, ensure_ascii=False, indent=2)
    
    logger.info(f"Processed batch metadata saved to: {manifest_path}")

    return manifest_path

def main() -> None:
    parser = argparse.ArgumentParser(description="Consume and transform a published book batch")
    parser.add_argument("--timeout", type=int, default=120)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    consume_books(timeout=args.timeout)


if __name__ == "__main__":
    main()