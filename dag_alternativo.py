from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago



dag = DAG(
    dag_id="dag_alternativo",
    start_date=days_ago(1),
    schedule_interval='0 23 * * *',
    catchup=False
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> end