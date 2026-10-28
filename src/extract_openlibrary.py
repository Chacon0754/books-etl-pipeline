import argparse
import json
import logging
import time
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

def extract_openlibrary(query: str = "subject:fiction", limit: int = 1000) -> Path:
    if limit < 1:
        raise ValueError("The record limit must be greater than zero")
    
    books = []
    page = 1
    page_size = min(100, limit)

    with requests.session() as session:
        session.headers.update({"User-Agent": "BooksETLPipeline/1.0"})

        while len(books) < limit:
            if page > 1:
                time.sleep(1.1)

    
            response = session.get(
                API_URL,
                params={
                    "q": query,
                    "page": page,
                    "limit": page_size,
                    "sort": "key",
                    "fields": ",".join(FIELDS)
                },
                timeout=60,
            )
            response.raise_for_status()

            payload = response.json()
            batch = payload.get("docs")



            if not isinstance(batch, list):
                raise ValueError(f"The API response for page {page} doesn't contain a book list")
    
            if not batch:
                break

            remainig = limit - len(books)
            books.extend(batch[:remainig])

            logger.info(
                f"Page {page}: received {len(batch)} books; "
                f"Collected {len(books)}/{limit}"
            )

            if len(batch) < page_size:
                break

            page += 1
    
    if not books:
        raise ValueError("No books were found for the requested query")
    
    if len(books) < limit:
        logger.warning(f"The query returned {len(books)} books below the requested limit of {limit}")
    
    result = {
        "query": query,
        "requested_count": limit,
        "extracted_count": len(books),
        "docs": books,
    }
    
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_PATH.open("w", encoding="utf-8") as output_file:
        json.dump(result, output_file, ensure_ascii=False, indent=2)
    
    logger.info(f"Extracted {len(books)} books from Openlibrary")
    logger.info(f"Raw data saved to {OUTPUT_PATH}")

    return OUTPUT_PATH

def main() -> None:
    parser = argparse.ArgumentParser(description="Extract bibliographic data from Openlibrary")

    parser.add_argument("--query", default="subject:fiction")
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    extract_openlibrary(query=args.query, limit=args.limit)

if __name__ == "__main__":
    main()