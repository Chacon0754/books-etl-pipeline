import logging
import os
from collections import Counter
from pathlib import Path
from statistics import fmean

from pymongo import MongoClient

from src.config import PROCESSED_DIRECTORY
from src.file_utils import read_json, write_json

logger = logging.getLogger(__name__)

LOAD_REPORT_PATH = PROCESSED_DIRECTORY / "mongodb_load.json"
METRICS_PATH = PROCESSED_DIRECTORY / "books_metrics.json"
PRICE_BIN_WIDTH = 5


def build_price_histogram(prices: list[float]) -> list[dict]:
    counts = Counter(
        int(price // PRICE_BIN_WIDTH) * PRICE_BIN_WIDTH
        for price in prices
    )

    return [
        {
            "start": start,
            "end": start + PRICE_BIN_WIDTH,
            "count": counts[start],
        }
        for start in range(
            min(counts),
            max(counts) + PRICE_BIN_WIDTH,
            PRICE_BIN_WIDTH,
        )
    ]


def build_metrics(books: list[dict], api_count: int) -> dict:
    if not books:
        raise ValueError("No scraped books are available for metrics.")

    prices = [book["price"] for book in books]
    ratings = [book["rating"] for book in books]

    category_counts = Counter(book["category"] for book in books)
    rating_counts = Counter(ratings)

    return {
        "api_count": api_count,
        "scraped_count": len(books),
        "currency": "GBP",
        "average_price": round(fmean(prices), 2),
        "minimum_price": min(prices),
        "maximum_price": max(prices),
        "average_rating": round(fmean(ratings), 2),
        "price_histogram": build_price_histogram(prices),
        "category_counts": [
            {"category": category, "count": count}
            for category, count in sorted(
                category_counts.items(),
                key=lambda item: (-item[1], item[0]),
            )
        ],
        "rating_counts": [
            {"rating": rating, "count": rating_counts[rating]}
            for rating in range(1, 6)
        ],
        "top_expensive": sorted(
            books,
            key=lambda book: (
                -book["price"],
                book["title"].casefold(),
                book["source_id"],
            ),
        )[:10],
        "top_rated": sorted(
            books,
            key=lambda book: (
                -book["rating"],
                book["title"].casefold(),
                book["source_id"],
            ),
        )[:10],
    }


def calculate_metrics() -> Path:
    load_report = read_json(LOAD_REPORT_PATH)
    batch_id = load_report["batch_id"]
    query = {"batch_id": batch_id}

    with MongoClient(
        os.environ["MONGO_URI"],
        serverSelectionTimeoutMS=10000,
    ) as client:
        database = client[os.environ["MONGO_DATABASE"]]

        api_count = database["books_api"].count_documents(query)
        books = list(
            database["books_scraped"].find(query, {"_id": 0})
        )

        actual_counts = {
            "books_api": api_count,
            "books_scraped": len(books),
        }

        for name, count in actual_counts.items():
            expected = load_report["collections"][name]["count"]

            if count != expected:
                raise RuntimeError(
                    f"Unexpected document count in {name}: "
                    f"{count}/{expected}."
                )

        metrics = {
            "_id": batch_id,
            "batch_id": batch_id,
            **build_metrics(books, api_count),
        }

        database["books_metricas"].replace_one(
            {"_id": batch_id},
            metrics,
            upsert=True,
        )

    write_json(METRICS_PATH, metrics)

    logger.info(
        f"Books: API={metrics['api_count']}, "
        f"scraped={metrics['scraped_count']}."
    )
    logger.info(
        f"Average price: GBP {metrics['average_price']:.2f}; "
        f"average rating: {metrics['average_rating']:.2f}."
    )
    logger.info(
        f"Categories: {len(metrics['category_counts'])}; "
        f"histogram total: "
        f"{sum(item['count'] for item in metrics['price_histogram'])}; "
        f"rating total: "
        f"{sum(item['count'] for item in metrics['rating_counts'])}."
    )
    logger.info(f"Metrics stored in books_metricas for batch {batch_id}.")
    logger.info(f"Metrics file saved to {METRICS_PATH}")

    return METRICS_PATH


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    calculate_metrics()


if __name__ == "__main__":
    main()