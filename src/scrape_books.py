import argparse
import json
import logging
import time
from pathlib import Path
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = PROJECT_ROOT / "data" / "raw" / "books_scraped.json"
BASE_URL = "https://books.toscrape.com/"

def fetch_page(session: requests.Session, url: str) -> BeautifulSoup:
    time.sleep(0.3)

    response = session.get(url, timeout=60)
    response.raise_for_status()

    return BeautifulSoup(response.content, "html.parser", from_encoding="utf-8")

def parse_books(card: Tag, category: str, page_url: str) -> dict:
    title = card.select_one("h3 a")
    price = card.select_one(".price_color")
    availability = card.select_one(".availability")
    rating = card.select_one(".star-rating")

    elements = (title, price, availability, rating)

    if any(element is None for element in elements):
        raise ValueError(f"Missing book fields on {page_url}")
    
    rating_value = next(
        (
            value for value in rating.get("class", [])
            if value in {"One", "Two", "Three", "Four", "Five"}
        ),
        None,
    )

    if rating_value is None:
        raise ValueError(f"Missing rating value on {page_url}")
    
    return {
        "url": urljoin(page_url, title["href"]),
        "title": title.get("title") or title.get_text(strip=True),
        "price": price.get_text(strip=True),
        "availability": " ".join(availability.stripped_strings),
        "rating": rating_value,
        "category": category,
    }


def scrape_books(limit: int = 1000) -> Path:
    if limit < 1:
        raise ValueError("The record limit must be greater than Zero")
    
    books = []
    seen_urls = set()

    with requests.Session() as session:
        session.headers.update({"User-Agent": "BooksELTPipeline/1.0"})

        homepage = fetch_page(session, BASE_URL)
        category_links = homepage.select(".side_categories ul li ul li a")

        if not category_links:
            raise ValueError("No category links were found")
        
        for category_link in category_links:
            category = category_link.get_text(strip=True)
            page_url = urljoin(BASE_URL, category_link["href"])

            while page_url and len(books) < limit:
                page = fetch_page(session, page_url)
                cards = page.select("article.product_pod")

                if not cards:
                    raise ValueError(f"No books were found on {page_url}")
                
                for card in cards:
                    book = parse_books(card, category, page_url)

                    if book["url"] in seen_urls:
                        continue

                    seen_urls.add(book["url"])
                    books.append(book)

                    if len(books) >= limit:
                        break
                
                logger.info(
                    f"Category: {category}; "
                    f"Collected {len(books)}/{limit} books"
                )

                next_link = page.select_one("li.next a")
                page_url = (
                    urljoin(page_url, next_link["href"])
                    if next_link
                    else None
                )

            if len(books) >= limit:
                break
        
    if not books:
        raise ValueError("No books were extracted")
    
    if len(books) < limit:
        logger.warning(f"Extracted: {len(books)} below the requested limit of {limit}")
    
    result = {
        "source": BASE_URL,
        "requested_count": limit,
        "extracted_count": len(books),
        "docs": books,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_PATH.open("w", encoding="utf-8") as output_file:
        json.dump(result, output_file, ensure_ascii=False, indent=2)
    
    logger.info(f"Extracted {len(books)} books from Books to Scrape")
    logger.info(f"Raw data saved to {OUTPUT_PATH}")

    return OUTPUT_PATH

def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape book data from Books to Scrape")

    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    scrape_books(limit=args.limit)

if __name__ == "__main__":
    main()