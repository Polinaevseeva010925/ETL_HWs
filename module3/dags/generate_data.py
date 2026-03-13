from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import random
from faker import Faker

fake = Faker()

def generate_user_sessions():
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['etl_db']
    collection = db['user_sessions']
    collection.delete_many({})
    sessions = []
    for i in range(100):
        sessions.append({
            "session_id": f"sess_{i:03d}",
            "user_id": f"user_{random.randint(1,50):03d}",
            "start_time": fake.date_time_this_year().isoformat(),
            "end_time": fake.date_time_this_year().isoformat(),
            "pages_visited": [fake.uri_path() for _ in range(random.randint(1,5))],
            "device": random.choice(["mobile", "desktop"]),
            "actions": random.choices(["login", "view_product", "add_to_cart", "logout"], k=random.randint(1,4))
        })
    collection.insert_many(sessions)

def generate_support_tickets():
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['etl_db']
    collection = db['support_tickets']
    collection.delete_many({})
    tickets = []
    for i in range(50):
        tickets.append({
            "ticket_id": f"ticket_{i:03d}",
            "user_id": f"user_{random.randint(1,50):03d}",
            "status": random.choice(["open", "closed", "pending"]),
            "issue_type": random.choice(["payment", "delivery", "product"]),
            "messages": [{"sender": "user", "message": fake.sentence(), "timestamp": fake.date_time_this_year().isoformat()}],
            "created_at": fake.date_time_this_year().isoformat(),
            "updated_at": fake.date_time_this_year().isoformat()
        })
    collection.insert_many(tickets)

dag = DAG(
    'generate_mongo_data',
    start_date=datetime(2026,3,1),
    schedule_interval=None,
    catchup=False
)

gen_sessions = PythonOperator(task_id='gen_sessions', python_callable=generate_user_sessions, dag=dag)
gen_tickets = PythonOperator(task_id='gen_tickets', python_callable=generate_support_tickets, dag=dag)

gen_sessions >> gen_tickets
