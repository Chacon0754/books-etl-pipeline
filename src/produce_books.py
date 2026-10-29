import argparse
from src.file_utils import read_json, write_json
import json
import logging
import os
from pathlib import Path
from uuid import uuid4
from confluent_kafka import Producer
from src.config import RAW_DIRECTORY
from typing import Any

logger = logging.getLogger(__name__)


BATCH_PATH = RAW_DIRECTORY / "kafka_batch.json"

SOURCES = {
    "books_api": ("openlibrary.json", "key"),
    "books_scraping": ("books_scraped.json", "url"),
}

def publish_topic(producer: Producer, topic: str, books: list, key_field: str, batch_id: str) -> dict:
    offsets = []
    errors = []

    def delivery_report(error, message):
        if error is not None:
            errors.append(str(error))
        else:
            offsets.append(message.offset())
    

    for book in books:
        event = {
            "batch_id": batch_id,
            "book": book
        }

        while True:
            try:
                producer.produce(
                    topic=topic,
                    partition=0,
                    key=book[key_field],
                    value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
                    on_delivery=delivery_report,
                )
                break
            except BufferError:
                producer.poll(1)
        
        producer.poll(0)
    
    pending = producer.flush(30)

    if pending or errors or len(offsets) != len(books):
        raise RuntimeError(
            f"Delivery failed for {topic}: "
            f"{len(offsets)}/{len(books)} delivered, "
            f"{pending} pending, {len(errors)} errors. "
            f"First error: {errors[:1]}"
        )
    
    return {
        "partition": 0,
        "count": len(offsets),
        "start_offset": min(offsets),
        "end_offset": max(offsets) + 1,
    }

def read_sources() -> dict[str, list[dict[str, Any]]]:
    datasets = {}

    for topic, (filename, key_field) in SOURCES.items():
        path = RAW_DIRECTORY / filename
        books = read_json(path).get("docs")

        if not isinstance(books, list) or not books:
            raise ValueError(f"No book records found in {path}")

        if any(not isinstance(book, dict) or not book.get(key_field) for book in books):
            raise ValueError(f"Missing book identifiers in {path}")
        
        datasets[topic] = books
    
    return datasets

def create_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "client.id": "books-producer",
            "enable.idempotence": True,
            "acks": "all",
            "delivery.timeout.ms": 30000,
        }
    )

def produce_books(batch_id: str | None = None) -> Path:
    batch_id = batch_id or uuid4().hex

    datasets = read_sources()
    producer = create_producer()

    batch: dict[str, Any] = {
        "batch_id": batch_id,
        "topics": {},
    }

    logger.info(f"Batch ID: {batch_id}")

    for topic, (_, key_field) in SOURCES.items():
        result = publish_topic(
            producer, topic, datasets[topic], key_field, batch_id
        )

        batch["topics"][topic] = result
        logger.info(f"Published {result['count']} messages to {topic}.")

    write_json(BATCH_PATH, batch)
    logger.info(f"Batch metadata saved to {BATCH_PATH}")

    return BATCH_PATH

def main() -> None:
    parser = argparse.ArgumentParser(description="Publish extracted book records to kafka")

    parser.add_argument("--batch-id")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    produce_books(batch_id=args.batch_id)
        
if __name__ == "__main__":
    main()