from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

def full_load():
    df = pd.read_csv('/opt/airflow/data/transformed_data.csv')
    print(f"Полная загрузка: {len(df)} строк")
    df.to_csv('/opt/airflow/data/full_load.csv', index=False)
    print("Полная загрузка завершена")

def delta_load():
    df = pd.read_csv('/opt/airflow/data/transformed_data.csv')
    df['noted_date'] = pd.to_datetime(df['noted_date'])
    cutoff_date = pd.to_datetime(datetime.now().date() - timedelta(days=3))
    delta_df = df[df['noted_date'] >= cutoff_date]
    print(f"Дельта загрузка: {len(delta_df)} строк за последние 3 дня")
    delta_df.to_csv('/opt/airflow/data/delta_load.csv', index=False)
    print("Дельта загрузка завершена")

default_args = {
    'owner': 'Polina Evseeva',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'etl_load_data',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

full_task = PythonOperator(
    task_id='full_load',
    python_callable=full_load,
    dag=dag,
)

delta_task = PythonOperator(
    task_id='delta_load',
    python_callable=delta_load,
    dag=dag,
)

full_task >> delta_task

