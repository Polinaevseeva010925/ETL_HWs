from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

def create_etl_db_and_tables():
    conn = psycopg2.connect(host='postgres', port=5432, dbname='postgres', user='admin', password='admin')
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS etl_db OWNER admin;")
    print("etl_db created/verified")
    cur.close()
    conn.close()
    
    conn = psycopg2.connect(host='postgres', port=5432, dbname='etl_db', user='admin', password='admin')
    cur
