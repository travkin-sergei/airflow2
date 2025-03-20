from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import datetime


def fetch_data():
    """Описание."""
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="123456",
        host="host.docker.internal",  # 👈 Подключение к локальному PostgreSQL
        port="5432"
    )
    cur = conn.cursor()
    cur.execute("SELECT * FROM description.data_table;")
    rows = cur.fetchall()
    for row in rows:
        print(row)
    cur.close()
    conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2024, 3, 20),
    "retries": 1,
}

dag = DAG(
    "hello_world_dag",
    default_args=default_args,
    schedule_interval="@daily",
)

fetch_task = PythonOperator(
    task_id="fetch_data",
    python_callable=fetch_data,
    dag=dag,
)
