import os
from datetime import datetime, timedelta

import requests
from airflow.decorators import dag, task
from pymongo import MongoClient, UpdateOne

API_URL = "https://api.frankfurter.dev/v2/rates"
BASE = "EUR"
QUOTES = "USD,GBP,JPY"


@dag(
    dag_id="currency_exchange_etl",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["etl", "currency"],
)
def currency_exchange_etl():

    @task
    def extract(ti=None):
        resp = requests.get(API_URL, params={"base": BASE, "quotes": QUOTES}, timeout=30)
        resp.raise_for_status()
        ti.xcom_push(key="raw_rates", value=resp.json())

    @task
    def transform(ti=None):
        raw = ti.xcom_pull(task_ids="extract", key="raw_rates")
        retrieved_at = datetime.utcnow().replace(microsecond=0).isoformat()

        if isinstance(raw, dict) and "rates" in raw:
            records = [
                {"date": raw["date"], "base": raw["base"], "quote": q, "rate": r}
                for q, r in raw["rates"].items()
            ]
        else:
            records = raw

        transformed = [
            {
                "base_currency": r["base"],
                "target_currency": r["quote"],
                "exchange_rate": r["rate"],
                "rate_date": r["date"],
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at,
            }
            for r in records
        ]
        ti.xcom_push(key="transformed_rates", value=transformed)

    @task
    def load(ti=None):
        docs = ti.xcom_pull(task_ids="transform", key="transformed_rates")
        client = MongoClient(os.environ["MONGO_URI"])
        collection = client[os.environ["MONGO_DB"]][os.environ["MONGO_COLLECTION"]]

        ops = [
            UpdateOne(
                {
                    "base_currency": d["base_currency"],
                    "target_currency": d["target_currency"],
                    "rate_date": d["rate_date"],
                },
                {"$set": d},
                upsert=True,
            )
            for d in docs
        ]
        if ops:
            collection.bulk_write(ops)
        client.close()

    extract() >> transform() >> load()


currency_exchange_etl()