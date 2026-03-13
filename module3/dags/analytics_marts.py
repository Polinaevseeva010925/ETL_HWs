from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag = DAG('analytics_marts', start_date=datetime(2026,3,1), schedule=None, catchup=False)

user_activity = PostgresOperator(
    task_id='user_activity_mart',
    postgres_conn_id='postgres_etl',
    sql="""
    CREATE TABLE IF NOT EXISTS user_activity_mart AS
    SELECT 
        user_id,
        COUNT(*) as session_count,
        AVG(session_duration) as avg_duration_sec,
        COUNT(DISTINCT unnest(pages_visited)) as unique_pages,
        string_agg(DISTINCT device, ', ') as devices_used
    FROM user_sessions 
    GROUP BY user_id;
    """,
    dag=dag
)

support_stats = PostgresOperator(
    task_id='support_stats_mart',
    postgres_conn_id='postgres_etl',
    sql="""
    CREATE TABLE IF NOT EXISTS support_stats_mart AS
    SELECT 
        issue_type,
        status,
        COUNT(*) as ticket_count,
        AVG(resolution_time)/3600.0 as avg_resolution_hours
    FROM support_tickets 
    GROUP BY issue_type, status;
    """,
    dag=dag
)

user_activity >> support_stats
