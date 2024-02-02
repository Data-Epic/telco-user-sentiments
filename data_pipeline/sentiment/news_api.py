from newsapi import NewsApiClient
import pandas as pd
import time
from dotenv import load_dotenv
import logging
import os

def fetch_articles(api_key, q, from_param, to, language='en', pages=100):
    """
    This function fetches data from news api for every page for the search.
    :param api_key: the key was gotten from news api developer website.
    :param q: the word you are trying to search for.
    :param from_param: the start date of the search
    :param to: the stop date of the search
    :param language: the language in which the article was written
    :param pages: what page the article is on for the search
    :return: a dictionary of all articles queried
    """
    newsapi = NewsApiClient(api_key=api_key)
    all_articles = []
    for page in range(1, pages + 1):
        try:
            response = newsapi.get_everything(q=q,
                                              from_param=from_param,
                                              to=to,
                                              language=language,
                                              page=page)
            r = r.json(response['articles'])
            logging.info(f"Successfully fetched articles for page {page}")

            if page < pages:
                logging.info("Waiting 3 seconds before fetching the next page...")
                time.sleep(3)
        except Exception as e:
            logging.error(f"Failed to fetch articles for page {page}: {e}")
            break
    print(all_articles)
    return all_articles

def process_articles(all_articles):
    '''
    This function converts the data to a dataframe and convert the source column into two columns(source_id, source_name)
    :param all_articles: A dictionary of the articles that contains the search word
    :return: a dataframe with 9 columns of the needed data
    '''
    df = pd.DataFrame(all_articles)
    if not df.empty:
        df['source_id'] = df['source'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
        df['source_name'] = df['source'].apply(lambda x: x.get('name') if isinstance(x, dict) else None)
        df = df.drop('source', axis=1)
    return df

def export_to_csv(df, file_name):
    '''
    This function exports the data to a csv file ready for preprocessing.
    :param df: the normalized dataframe
    :param file_name: The name/title for the data to be exported into.
    :return: a csv file
    '''
    df.to_csv(file_name, index=False)


if __name__ == '__main__':
    load_dotenv()
    LOG_PATH = os.getenv("LOG_PATH")
    LOG_INFO_PATH = os.getenv("LOG_INFO_PATH")
    logging.basicConfig(filename=LOG_PATH, level=logging.ERROR,
                        format='%(levelname)s (%(asctime)s) : %(message)s (%(lineno)d)')
    logging.basicConfig(filename=LOG_INFO_PATH, level=logging.INFO,
                        format='%(levelname)s (%(asctime)s) : %(message)s (%(lineno)d)')
    API_KEY = os.getenv('API_KEY')
    api_key = API_KEY
    all_articles = fetch_articles(api_key, 'tesla', '2023-12-28', '2024-01-28', 'en', 12)
    final_df = process_articles(all_articles)
    export_to_csv(final_df, 'news_articles.csv')

    print("Exported the DataFrame to 'news_articles.csv'")


