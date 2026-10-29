from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task, get_current_context
from airflow.providers.standard.operators.empty import EmptyOperator

@dag(
    dag_id="books_etl_pipeline",
    description="Book extraction, processing, storage and visualization",
    start_date = datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
        "execution_timeout": timedelta(minutes=15),
    },
    tags=["books", "etl"],
)
def books_etl_pipeline() -> None:

    @task
    def extract_openlibrary_api() -> str:
        from src.extract_openlibrary import extract_openlibrary

        return str(
            extract_openlibrary(query="subject:fiction", limit=1000)
        )
    
    @task
    def scrape_books_to_scrape() -> str:
        from src.scrape_books import scrape_books

        return str(scrape_books(limit=1000))

    @task
    def send_books_to_kafka() -> str:
        from src.produce_books import produce_books

        context = get_current_context()
        return str(produce_books(batch_id=context["run_id"]))
    
    @task
    def consume_transform_books() -> str:
        from src.consume_books import consume_books

        return str(consume_books())
    
    @task
    def load_books_mongodb() -> str:
        from src.load_books import load_books

        return str(load_books())
    
    @task
    def calculate_books_metrics() -> str:
        from src.calculate_metrics import calculate_metrics

        return str(calculate_metrics())
    
    @task
    def refresh_streamlit_dashboard() -> str:
        from src.refresh_dashboard import refresh_dashboard

        return str(refresh_dashboard())
    

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    api = extract_openlibrary_api()
    scraping = scrape_books_to_scrape()
    published = send_books_to_kafka()
    transformed = consume_transform_books()
    loaded = load_books_mongodb()
    metrics = calculate_books_metrics()
    dashboard = refresh_streamlit_dashboard()

    start >> [api, scraping] >> published >> transformed >> loaded >> metrics >> dashboard >> end


books_etl_pipeline()