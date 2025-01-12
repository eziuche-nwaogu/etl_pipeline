from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy_utils import create_database, database_exists
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
from datetime import timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
log = LoggingMixin().log

@dag(
    dag_id='bank_scraper_etl',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 12, 31, 17, 30),
    catchup=False,
    tags=['example', 'bank', 'etl']
)
def bank_scraper_etl():

    @task
    def extract():
        url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
        html_page = requests.get(url).text
        data = BeautifulSoup(html_page, 'html.parser')
        tables = data.find_all('tbody')
        rows = tables[0].find_all('tr')

        table_data = []
        for row in rows:
            col = row.find_all('td')
            if len(col) != 0:
                if col[1].find('a') is not None:
                    table_data.append({'Bank_name': col[1].get_text(strip=True),
                                       'Gdp_in_Billion': col[2].get_text(strip=True)})

        print(f"Extracted data: {table_data}")
        log.info(f"Extracted data: {table_data}")
        return table_data

    @task
    def transform(data):
        df = pd.DataFrame(data)
        bank_data1 = df['Bank_name']
        bank_data2 = df['Gdp_in_Billion'].tolist()
        dollar_data = [float(item) for item in bank_data2]
        euro_data = [round(float(item) * 0.93, 2) for item in bank_data2]
        gbp_data = [round(float(item) * 0.8, 2) for item in bank_data2]
        inr_data = [round(float(item) * 82.95, 2) for item in bank_data2]
        df = df.drop(columns=['Gdp_in_Billion'])

        df['Bank_name'] = bank_data1
        df['Market_cap_in_$Billion'] = dollar_data
        df['Market_cap_in_eu_billion'] = euro_data
        df['Market_cap_in_gbp_billion'] = gbp_data
        df['Market_cap_in_inr_billion'] = inr_data

        print(f"Transformed DataFrame:\n{df}")
        log.info(f"Transformed DataFrame: {df}")
        return df

    @task
    def load(df):
        # Fetch the PostgreSQL connection
        connection = BaseHook.get_connection('my_postgres_conn')  # Use the configured connection ID
        uid = connection.login
        pwd = connection.password
        host = connection.host
        port = connection.port
        schema = connection.schema

        # Build the PostgreSQL URL
        postgres_url = f'postgresql+psycopg2://{uid}:{pwd}@{host}:{port}/{schema}'

        # Create the database if it doesn't exist
        if not database_exists(postgres_url):
            create_database(postgres_url)

        # Load the DataFrame into PostgreSQL
        engine = create_engine(postgres_url)
        table_name = 'Bank_Cap'
        df.to_sql(table_name, engine, if_exists='replace', index=False)

        # Optionally save the DataFrame to a CSV file
        df.to_csv('my_largest_bank.csv')
        print(f"Postgres URL: {postgres_url}")
        print(f"DataFrame to load:\n{df}")
        log.info(f"Postgres URL: {postgres_url}")
        log.info(f"DataFrame to load: {df}")

    # Define task execution order
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data)


bank_scraper_etl()
