import math

RATING_MAP = {
    "One": 1,
    "Two": 2,
    "Three": 3,
    "Four": 4,
    "Five": 5
}

def clean_text(value: str) -> str:
    return " ".join(value.split())

def transform_api_book(book: dict) -> dict:
    year = book.get("first_publish_year")

    return {
        "source_id": book["key"],
        "title": clean_text(book["title"]),
        "authors": [clean_text(name) for name in (book.get("author_name") or [])],
        "author_ids": book.get("author_key") or [],
        "first_publish_year": int(year) if year is not None else None,
        "subjects": [clean_text(subject).casefold() for subject in (book.get("subject") or [])],
        "isbn": book.get("isbn") or [],
    }

def transform_scraped_book(book: dict) -> dict:
    raw_price = book["price"].strip()

    if not raw_price.startswith("£"):
        raise ValueError(f"Unexpected price currency: {raw_price}")
    
    price = float(raw_price.removeprefix("£").strip())

    if not math.isfinite(price) or price < 0:
        raise ValueError(f"Invalid book price: {raw_price}")
    
    raw_rating = book["rating"]

    if raw_rating not in RATING_MAP:
        raise ValueError(f"Invalid book rating: {raw_rating}")
    
    availability = clean_text(book["availability"]).casefold()

    if availability not in {"in stock", "out of stock"}:
        raise ValueError(f"Unknown availability: {availability}")
    
    category = clean_text(book["category"]).casefold()

    if not category:
        raise ValueError("The book category is empty")
    
    return {
        "source_id": book["url"],
        "url": book["url"],
        "title": clean_text(book["title"]),
        "price": price,
        "currency": "GBP",
        "rating": RATING_MAP[raw_rating],
        "available": availability == "in stock",
        "category": category,
    }