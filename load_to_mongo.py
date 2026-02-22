from airflow.sdk import DAG, Asset
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
import pandas as pd
from datetime import datetime

processed_dataset_path = Variable.get("processed_dataset_path")
processed_asset = Asset(processed_dataset_path)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 2, 17),
    "retries": 1,
}

def load_to_mongo_func():
    df = pd.read_csv(processed_dataset_path)
    hook = MongoHook(mongo_conn_id="mongo_conn")
    collection = hook.get_collection("reviews", "reviews_db")
    records = df.to_dict(orient="records")
    if records:
        collection.insert_many(records)
    else:
        print("No records to insert into MongoDB.")

with DAG(
    dag_id="load_reviews_to_mongo",
    default_args=default_args,
    schedule=[processed_asset],
    catchup=False,
) as dag:

    load_to_mongo = PythonOperator(
        task_id="load_to_mongo",
        python_callable=load_to_mongo_func,
    )
