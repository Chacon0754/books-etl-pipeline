import logging
import os
from datetime import datetime

import altair as alt
import pandas as pd
import streamlit as st
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from src.config import PROCESSED_DIRECTORY
from src.file_utils import read_json

logger = logging.getLogger(__name__)
STATE_PATH = PROCESSED_DIRECTORY / "dashboard_state.json"


def load_dashboard_data(batch_id: str) -> tuple[dict, pd.DataFrame]:
    query = {"batch_id": batch_id}

    with MongoClient(
        os.environ["MONGO_URI"],
        serverSelectionTimeoutMS=10000,
    ) as client:
        database = client[os.environ["MONGO_DATABASE"]]
        metrics = database["books_metricas"].find_one(
            {"_id": batch_id}, {"_id": 0}
        )
        api_books = list(database["books_api"].find(query, {"_id": 0}))
        scraped_books = list(
            database["books_scraped"].find(query, {"_id": 0})
        )

    if metrics is None:
        raise ValueError("The published metrics are not available.")

    if (
        len(api_books) != metrics["api_count"]
        or len(scraped_books) != metrics["scraped_count"]
    ):
        raise ValueError("The published batch is not complete.")

    rows = [
        {
            "source": "Open Library",
            "title": book["title"],
            "author": ", ".join(book["authors"]),
            "category": "; ".join(book["subjects"]),
            "year": book["first_publish_year"],
            "price": None,
            "rating": None,
        }
        for book in api_books
    ]

    rows.extend(
        {
            "source": "Books to Scrape",
            "title": book["title"],
            "author": "",
            "category": book["category"],
            "year": None,
            "price": book["price"],
            "rating": book["rating"],
        }
        for book in scraped_books
    )

    table = pd.DataFrame(rows)
    table["year"] = pd.array(table["year"], dtype="Int64")
    table["rating"] = pd.array(table["rating"], dtype="Int64")

    return metrics, table


def show_summary(metrics: dict) -> None:
    api, scraped, price, rating = st.columns(4)

    api.metric("API books", metrics["api_count"])
    scraped.metric("Scraped books", metrics["scraped_count"])
    price.metric("Average price (GBP)", f"{metrics['average_price']:.2f}")
    rating.metric("Average rating", f"{metrics['average_rating']:.2f} / 5")


def show_ranking(books: list[dict], field: str, title: str) -> None:
    st.subheader(title)

    data = pd.DataFrame(books)
    data["label"] = [
        f"{number}. {name}"
        for number, name in enumerate(data["title"], 1)
    ]

    chart = alt.Chart(data).mark_bar().encode(
        x=alt.X(
            f"{field}:Q",
            title="Price (GBP)" if field == "price" else "Rating",
        ),
        y=alt.Y("label:N", sort=None, title=None),
        tooltip=["title:N", "category:N", "price:Q", "rating:O"],
    ).properties(height=360)

    st.altair_chart(chart, width="stretch")


def show_charts(metrics: dict) -> None:
    left, right = st.columns(2)

    with left:
        st.subheader("Price histogram")

        histogram = alt.Chart(
            pd.DataFrame(metrics["price_histogram"])
        ).mark_bar().encode(
            x=alt.X("start:Q", bin="binned", title="Price (GBP)"),
            x2=alt.X2("end:Q"),
            y=alt.Y("count:Q", title="Books"),
            tooltip=["start:Q", "end:Q", "count:Q"],
        ).properties(height=280)

        st.altair_chart(histogram, width="stretch")

    with right:
        st.subheader("Rating distribution")

        ratings = alt.Chart(
            pd.DataFrame(metrics["rating_counts"])
        ).mark_bar().encode(
            x=alt.X("rating:O", title="Rating"),
            y=alt.Y("count:Q", title="Books"),
            tooltip=["rating:O", "count:Q"],
        ).properties(height=280)

        st.altair_chart(ratings, width="stretch")

    st.subheader("Books by category")

    categories = alt.Chart(
        pd.DataFrame(metrics["category_counts"])
    ).mark_bar().encode(
        x=alt.X("count:Q", title="Books"),
        y=alt.Y("category:N", sort="-x", title="Category"),
        tooltip=["category:N", "count:Q"],
    ).properties(height=800)

    st.altair_chart(categories, width="stretch")

    left, right = st.columns(2)

    with left:
        show_ranking(
            metrics["top_expensive"],
            "price",
            "10 most expensive books",
        )

    with right:
        show_ranking(
            metrics["top_rated"],
            "rating",
            "10 highest-rated books",
        )


def show_table(table: pd.DataFrame) -> None:
    st.subheader("Explore books")
    st.caption(
        "Authors come from Open Library. "
        "Prices and ratings come from Books to Scrape."
    )

    source = st.selectbox(
        "Source",
        ["All", "Open Library", "Books to Scrape"],
        key="source_filter",
    )

    title_column, author_column, category_column = st.columns(3)
    title = title_column.text_input("Title", key="title_filter")
    author = author_column.text_input("Author", key="author_filter")
    category = category_column.text_input(
        "Category / subject",
        key="category_filter",
    )

    filtered = (
        table if source == "All"
        else table[table["source"] == source]
    )

    for field, value in [
        ("title", title),
        ("author", author),
        ("category", category),
    ]:
        if value.strip():
            filtered = filtered[
                filtered[field].str.contains(
                    value.strip(),
                    case=False,
                    regex=False,
                    na=False,
                )
            ]

    st.caption(
        f"Showing {len(filtered):,} of {len(table):,} records. "
        "Filters apply to this table."
    )

    st.dataframe(
        filtered,
        hide_index=True,
        width="stretch",
        height=420,
        column_config={
            "source": "Source",
            "title": "Title",
            "author": "Author",
            "category": "Category / subject",
            "year": st.column_config.NumberColumn(
                "Publication year", format="%d"
            ),
            "price": st.column_config.NumberColumn(
                "Price (GBP)", format="%.2f"
            ),
            "rating": st.column_config.NumberColumn(
                "Rating", format="%d"
            ),
        },
    )


@st.fragment(run_every="10s")
def show_dashboard() -> None:
    try:
        state = read_json(STATE_PATH)
        metrics, table = load_dashboard_data(state["batch_id"])
    except FileNotFoundError:
        st.info(
            "No data has been published yet. "
            "Run the pipeline to prepare the first batch."
        )
        return
    except ValueError:
        st.info(
            "The published batch is being updated. "
            "Please wait a few seconds."
        )
        return
    except PyMongoError:
        logger.exception("Could not load dashboard data")
        st.error(
            "Could not connect to the database. "
            "Check that MongoDB is available."
        )
        return

    updated_at = datetime.fromisoformat(state["updated_at"])
    st.caption(f"Updated: {updated_at:%Y-%m-%d %H:%M} UTC")

    show_summary(metrics)
    show_charts(metrics)
    show_table(table)


def main() -> None:
    st.set_page_config(page_title="Book Analysis", layout="wide")
    st.title("Book Analysis")
    st.caption(
        "Prices and ratings describe the "
        "Books to Scrape demonstration catalog."
    )

    show_dashboard()


if __name__ == "__main__":
    main()