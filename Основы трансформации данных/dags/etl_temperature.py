from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

def load_and_transform():
    df = pd.read_csv('/opt/airflow/data/data.csv')
    
    print(f"Исходные данные: {len(df)} строк")
    
    df = df[df['out/in'].str.strip().str.lower() == 'in'].copy()
    print(f"После фильтрации: {len(df)} строк")
    
    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M')
    df['noted_date'] = df['noted_date'].dt.date
    df['noted_date'] = df['noted_date'].astype(str)
    
    q5 = df['temp'].quantile(0.05)
    q95 = df['temp'].quantile(0.95)
    print(f"5-й процентиль: {q5:.1f}, 95-й процентиль: {q95:.1f}")
    
    df = df[(df['temp'] >= q5) & (df['temp'] <= q95)].copy()
    print(f"После очистки: {len(df)} строк")
    
    df.to_csv('/opt/airflow/data/transformed_data.csv', index=False)

def find_extremes():
    df = pd.read_csv('/opt/airflow/data/transformed_data.csv')
    
    daily_temp = df.groupby('noted_date')['temp'].mean().reset_index()
    daily_temp.columns = ['date', 'avg_temp']
    
    hottest_5 = daily_temp.nlargest(5, 'avg_temp')
    coldest_5 = daily_temp.nsmallest(5, 'avg_temp')
    
    print("\nСамые жаркие дни:")
    print(hottest_5.to_string(index=False))
    
    print("\nСамые холодные дни:")
      print(coldest_5.to_string(index=False))
    
    hottest_5.to_csv('/opt/airflow/data/hottest_5_days.csv', index=False)
    coldest_5.to_csv('/opt/airflow/data/coldest_5_days.csv', index=False)

default_args = {
    'owner': 'Polina_Evseeva',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'etl_temperature_data',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

task1 = PythonOperator(
    task_id='transform_data',
    python_callable=load_and_transform,
    dag=dag,
)

task2 = PythonOperator(
    task_id='find_extremes',
    python_callable=find_extremes,
    dag=dag,
)

task1 >> task2

