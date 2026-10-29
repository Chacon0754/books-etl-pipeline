import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient

from src.config import PROCESSED_DIRECTORY
from src.file_utils import read_json, write_json

logger = logging.getLogger(__name__)

METRICS_PATH = PROCESSED_DIRECTORY / "books_metrics.json"
DASHBOARD_STATE_PATH = PROCESSED_DIRECTORY / "dashboard_state.json"


def refresh_dashboard() -> Path:
    batch_id = read_json(METRICS_PATH)["batch_id"]

    with MongoClient(
        os.environ["MONGO_URI"],
        serverSelectionTimeoutMS=10000,
    ) as client:
        database = client[os.environ["MONGO_DATABASE"]]

        metrics = database["books_metricas"].find_one(
            {"_id": batch_id}
        )

        if metrics is None:
            raise ValueError(f"No metrics found for batch {batch_id}")

        expected_counts = {
            "books_api": metrics["api_count"],
            "books_scraped": metrics["scraped_count"],
        }

        for name, expected in expected_counts.items():
            count = database[name].count_documents(
                {"batch_id": batch_id}
            )

            if count != expected:
                raise RuntimeError(
                    f"Dashboard data is incomplete in {name}: "
                    f"{count}/{expected} documents."
                )

    state = {
        "batch_id": batch_id,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    temporary_path = DASHBOARD_STATE_PATH.with_suffix(".tmp")
    write_json(temporary_path, state)
    temporary_path.replace(DASHBOARD_STATE_PATH)

    logger.info(f"Dashboard prepared for batch {batch_id}.")
    logger.info(f"Dashboard state saved to {DASHBOARD_STATE_PATH}")

    return DASHBOARD_STATE_PATH


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    refresh_dashboard()


if __name__ == "__main__":
    main()