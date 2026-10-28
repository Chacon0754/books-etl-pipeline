import argparse
import json
import logging
from pathlib import Path
import requests

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = PROJECT_ROOT / "data" / "raw" / "openlibrary.json"
API_URL = "https://openlibrary.org/search.json"

FIELDS = (
    "key",
    "title",
    "author_key",
    "author_name",
    "first_publish_year",
    "subject",
    "isbn",
)

def extract_openlibrary(query: str = "subject:fiction", limit: int = 100) -> Path:
    if limit < 1:
        raise ValueError("The record limit must be greater than zero")
    
    response = requests.get(
        API_URL,
        params={
            "q": query,
            "limit": limit,
            "fields": ",".join(FIELDS)
        },
        headers={"User-Agent": "BooksETLPipeline/1.0"},
        timeout=60,
    )

    response.raise_for_status()

    payload = response.json()
    books = payload.get("docs")

    if not isinstance(books, list):
        raise ValueError("The API response doesn't contain a book list")
    
    if not books:
        raise ValueError("No books were found for the requested query")
    
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_PATH.open("w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, ensure_ascii=False, indent=2)
    
    logger.info(f"Extracted {len(books)} books from Openlibrary")
    logger.info(f"Raw data saved to {OUTPUT_PATH}")

    return OUTPUT_PATH

def main() -> None:
    parser = argparse.ArgumentParser(description="Extract bibliographic data from Openlibrary")

    parser.add_argument("--query", default="subject:fiction")
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    extract_openlibrary(query=args.query, limit=args.limit)

if __name__ == "__main__":
    main()