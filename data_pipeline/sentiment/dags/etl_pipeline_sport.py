import pendulum
from airflow.decorators import dag, task
import logging
from airflow.providers.mongo.hooks.mongo import MongoHook
import polars as pl

@dag(
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=['Loading cleaned sport data into Airflow'],
)
def etl_sport_data_into_new_mongodb():
    @task
    def extract_crypto_data_from_mongodb():
        try:
            hook = MongoHook(mongo_conn_id="mongo_default")
            client = hook.get_conn()
            db = client.trending_data
            collection = db.raw_sport
            documents = list(collection.find({}, {'_id': 0}))
            values = [document for document in documents]
            logging.info(f"Extracted {len(documents)} documents from MongoDB")
        except Exception as e:
            logging.error(f"Error extracting from MongoDB: {e}")
        return values

    @task
    def transform_data(values):
        if values:
            df = pl.DataFrame(values)
        col = ['author', 'title', 'description', 'url', 'urlToImage', 'publishedAt', 'content']
        df = df.select(
            pl.col("source").struct.field("id"),
            pl.col("source").struct.field("name"), pl.col(col)
        )
        df = df.unique(maintain_order=True)
        columns = ['url', 'urlToImage']
        df = df.drop(columns)
        df = df.with_columns([pl.col("content").str.split_exact("…", 1).struct.rename_fields(
            ["content_orig", "discard"]).alias("fields"), ]).unnest("fields")
        df = df.with_columns([pl.col("description").str.split_exact("…", 1).struct.rename_fields(
            ["description_orig", "discard2"]).alias("fields2"), ]).unnest("fields2")
        df = df.with_columns([pl.col("title").str.split_exact("…", 1).struct.rename_fields(
            ["title_orig", "discard3"]).alias("fields3"), ]).unnest("fields3")

        columns2 = ['content', 'discard', 'discard2', 'discard3', 'description', 'title']
        df = df.drop(columns2)
        df = df.rename({"content_orig": "content"})
        df = df.rename({"description_orig": "description"})
        df = df.rename({"title_orig": "title"})
        transformed_documents = df.to_dicts()
        return transformed_documents

    @task
    def load_raw_data(transformed_documents: list):
        if not transformed_documents:
            logging.info("No documents to load into MongoDB.")
            return

        try:
            hook = MongoHook(mongo_conn_id="mongo_default")
            client = hook.get_conn()
            db = client.trending_data
            collection = db.cleaned_sport
            logging.info(f"Connected to MongoDB - {client.server_info()}")

            for article in transformed_documents:

                unique_condition = {'description': article['description']}
                collection.update_one(unique_condition, {'$set': article}, upsert=True)
                logging.info("Article successfully upserted into MongoDB")
        except Exception as e:
            logging.error(f"Error connecting to or inserting into MongoDB: {e}")

    documents = extract_crypto_data_from_mongodb()
    transformed_documents = transform_data(documents)
    load_raw_data(transformed_documents)

etl_sport_data_into_new_mongodb()
