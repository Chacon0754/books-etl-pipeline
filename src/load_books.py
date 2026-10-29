from src.file_utils import read_json, write_json
import logging
import os
from pathlib import Path
from pymongo import MongoClient, ReplaceOne
from src.config import PROCESSED_DIRECTORY

logger  = logging.getLogger(__name__)

MANIFEST_PATH = PROCESSED_DIRECTORY / "processed_batch.json"
LOAD_REPORT_PATH = PROCESSED_DIRECTORY / "mongodb_load.json"

COLLECTIONS = ("books_api", "books_scraped")

def load_books() -> Path:
    manifest = read_json(MANIFEST_PATH)
    
    batch_id = manifest["batch_id"]
    datasets = {}
    
    for collection_name in COLLECTIONS:
        specification = manifest["collections"][collection_name]
        input_path = PROCESSED_DIRECTORY / specification["file"]

        payload = read_json(input_path)
        
        if payload.get("batch_id") != batch_id:
            raise ValueError(f"Batch mismatch in {input_path}")

        books = payload.get("docs")

        if not isinstance(books, list) or not books:
            raise ValueError(f"No processed books found in {input_path}")

        if len(books) != specification["count"]:
            raise ValueError(f"Unexpected record count in {input_path}")

        identifiers = [book["source_id"] for book in books]

        if len(set(identifiers)) != len(identifiers):
            raise ValueError(f"Duplicate identifiers in {input_path}")
        
        datasets[collection_name] = books
        
    database_name = os.environ["MONGO_DATABASE"]

    report = {
        "batch_id": batch_id,
        "database": database_name,
        "collections": {},
    }

    with MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMs=10000) as client:
        client.admin.command("ping")
        database = client[database_name]

        for collection_name, books in datasets.items():
            collection = database[collection_name]

            operations = [
                ReplaceOne(
                    {"_id": book["source_id"]},
                    {
                        "_id": book["source_id"],
                        **book,
                        "batch_id": batch_id,
                    },
                    upsert=True
                )
                for book in books
            ]

            result = collection.bulk_write(operations, ordered=False)
            batch_count = collection.count_documents({"batch_id": batch_id})

            if batch_count != len(books):
                raise RuntimeError(
                    f"MongoDB count mismatch in {collection_name}: "
                    f"{batch_count}/{len(books)} documents."
                )
            
            report["collections"][collection_name] = {
                "count": batch_count,
                "upserted_count": result.upserted_count,
                "matched_count": result.matched_count,
                "modified_count": result.modified_count,
            }

            logger.info(
                f"{collection_name}: "
                f"upserted={result.upserted_count}, "
                f"matched={result.matched_count}, "
                f"modified={result.modified_count}, "
                f"batch_count={batch_count}."
            )
    
    write_json(LOAD_REPORT_PATH, report)
    
    logger.info(f"MongoDB load report saved to {LOAD_REPORT_PATH}")

    return LOAD_REPORT_PATH



def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    load_books()


if __name__ == "__main__":
    main()