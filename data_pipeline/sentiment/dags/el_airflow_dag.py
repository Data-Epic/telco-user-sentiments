# imports important for Airflow
import pendulum
from airflow.decorators import dag, task
import logging
import time
from newsapi import NewsApiClient
from airflow.providers.mongo.hooks.mongo import MongoHook

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=['Loading Raw data into Airflow'],
)
def el_news_data_into_mongodb():
    @task
    def extract_from_newsapi(q, from_param, to, language, pages):
        
        newsapi = NewsApiClient(api_key='188877b9bd2741479357336cdb5bc761')
        all_articles = []
        for page in range(pages, pages + 4):
            response = newsapi.get_everything(q=q,
                                              from_param=from_param,
                                              to=to,
                                              language=language,
                                              page=page)
            articles = response['articles']
            logging.info(f"Successfully fetched articles for page {page}")
            all_articles.append(articles)
            if page < pages + 3:
                logging.info("Waiting 3 seconds before fetching the next page...")
                time.sleep(3)
        return all_articles

    @task
    def load_raw_data(all_articles: list):
        try:
            hook = MongoHook(mongo_conn_id="mongo_default")
            client = hook.get_conn()
            db = client.trending_data
            collection = db.raw_electric_vehicle
            logging.info(f"Connected to MongoDB - {client.server_info()}")
            for article in all_articles:
                collection.insert_many(article)
                logging.info("Articles successfully inserted into MongoDB")
        except Exception as e:
            logging.error(f"Error connecting to or inserting into MongoDB: {e}")

    all_articles = extract_from_newsapi("tesla", "2024-01-01", "2024-01-29", "en", 1)
    load_raw_data(all_articles)


el_raw_collection_dag_mongodb = el_news_data_into_mongodb()
