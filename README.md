# Pipeline ETL de libros y precios

Proyecto de Big Data que obtiene información bibliográfica desde Open Library y datos comerciales desde Books to Scrape. Apache Airflow orquesta la extracción, el procesamiento mediante Kafka, la carga en MongoDB y la preparación de los resultados para Streamlit.

Se procesan 1,000 libros de cada fuente.

## Fuentes de datos

- [Open Library](https://openlibrary.org/developers/api): títulos, autores, años de publicación, temas e identificadores disponibles.
- [Books to Scrape](https://books.toscrape.com/): títulos, precios, disponibilidad, ratings y categorías.

Books to Scrape es un sitio de práctica. Sus precios y ratings no representan información del mercado real.

Los registros de ambas fuentes se conservan por separado. No se presupone que un libro de Open Library corresponda a un producto de Books to Scrape.

## Flujo del pipeline

El DAG `books_etl_pipeline` contiene nueve tareas:

1. `start`: inicio.
2. `extract_openlibrary_api`: extracción de 1,000 libros desde Open Library.
3. `scrape_books_to_scrape`: extracción de 1,000 libros mediante scraping.
4. `send_books_to_kafka`: publicación de ambas fuentes en Kafka.
5. `consume_transform_books`: consumo, validación y transformación.
6. `load_books_mongodb`: carga de los libros en MongoDB.
7. `calculate_books_metrics`: cálculo y almacenamiento de métricas.
8. `refresh_streamlit_dashboard`: publicación del lote disponible para visualizar.
9. `end`: finalización.

Las dos extracciones se ejecutan en paralelo. Las etapas posteriores se ejecutan secuencialmente.

El DAG utiliza los decoradores `@dag` y `@task` de `airflow.sdk`. Su ejecución es manual y admite un máximo de una ejecución activa para evitar que dos ejecuciones escriban simultáneamente en los mismos archivos.

## Tecnologías

| Componente | Configuración |
|---|---|
| Apache Airflow | 3.3.2, TaskFlow API y LocalExecutor |
| Apache Kafka | 4.2.2, modo KRaft |
| MongoDB | 8.0 |
| Streamlit | 1.66.1 |
| Python en contenedores | 3.12 |
| Python del entorno local utilizado | 3.14.3 |
| Despliegue | Docker Compose |

Las dependencias están definidas en:

- `requirements.txt`: extracción y procesamiento.
- `requirements.dashboard.txt`: dashboard.
- `requirements.dev.txt`: Airflow para el entorno local de desarrollo.

## Servicios

| Servicio | Dirección |
|---|---|
| Interfaz de Airflow | http://localhost:8080 |
| Dashboard de Streamlit | http://localhost:8501 |
| MongoDB desde el equipo anfitrión | `127.0.0.1:27018` |
| MongoDB entre contenedores | `mongodb:27017` |
| Kafka entre contenedores | `kafka:9092` |

Airflow, Kafka, MongoDB y Streamlit se ejecutan dentro de Docker.

La instalación local de Python permite desarrollar y comprobar los módulos. La ejecución completa del pipeline se realiza mediante Airflow dentro del contenedor.

## Kafka y MongoDB

Los tópicos de Kafka son:

- `books_api`
- `books_scraping`

La base de datos `books_pipeline` contiene:

| Colección | Contenido |
|---|---|
| `books_api` | Datos bibliográficos transformados |
| `books_scraped` | Datos comerciales transformados |
| `books_metricas` | Métricas de cada lote |

El identificador de la ejecución de Airflow se utiliza como `batch_id`.

Los libros se cargan mediante operaciones de reemplazo con `upsert`, utilizando identificadores estables. Las métricas se almacenan en un documento por lote.

## Dashboard

Streamlit presenta:

- Histograma de precios.
- Cantidad de libros por categoría.
- Distribución de ratings.
- Los diez libros más caros.
- Los diez libros mejor calificados.
- Tabla con filtros por fuente, título, autor y categoría o tema.

Las gráficas de precios, ratings y categorías comerciales utilizan los datos de Books to Scrape. Los autores y temas bibliográficos provienen de Open Library.

Con una sesión activa, el dashboard comprueba cada diez segundos el lote publicado por el pipeline.

## Estructura

```text
app/
    dashboard.py
dags/
    books_etl_pipeline.py
src/
    config.py
    file_utils.py
    extract_openlibrary.py
    scrape_books.py
    produce_books.py
    transform_books.py
    consume_books.py
    load_books.py
    calculate_metrics.py
    refresh_dashboard.py
data/
    raw/
    processed/
compose.yaml
Dockerfile.airflow
Dockerfile.dashboard
requirements.txt
requirements.dashboard.txt
requirements.dev.txt
README.md
REPRODUCCION.md
```

Los archivos de `data/` se generan durante la ejecución.

El reporte LaTeX y sus capturas se mantienen en un repositorio independiente.

## Configuración y reproducción

Se requiere Docker Desktop en ejecución y Docker Compose.

Las variables de configuración se definen en un archivo `.env` ubicado en la raíz del proyecto:

- `AIRFLOW_UID`
- `AIRFLOW_JWT_SECRET`
- `AIRFLOW_API_SECRET_KEY`
- `MONGO_ROOT_USERNAME`
- `MONGO_ROOT_PASSWORD`
- `MONGO_DATABASE`

El archivo `.env` está excluido de Git.

Consulta [REPRODUCCION.md](REPRODUCCION.md) para configurar el entorno, construir las imágenes, crear los tópicos, ejecutar el DAG y comprobar los resultados.

## Validación realizada

Se comprobó una ejecución completa con las nueve tareas en estado `success`.

Para ese lote se verificaron:

- 1,000 documentos en `books_api`.
- 1,000 documentos en `books_scraped`.
- Un documento del lote en `books_metricas`.
- 50 categorías.
- Precio promedio de 35.07 GBP.
- Rating promedio de 2.92.
- Coincidencia entre el lote de Airflow y el publicado para Streamlit.
- Consultas de los documentos mediante MongoDB Compass.

Estos valores corresponden al conjunto de datos utilizado en la validación.