from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dagrun_operator import TriggerDagRunOperator


############################################################################ Functions ################################################

def python_branch(**context):
    var=0
    if var == 0:
        return 'tarea_1'
    else:
        return 'tarea_alterna'

def push(**kwargs):    
    result = "Hola mundo"
    kwargs["ti"].xcom_push(key = 'push_result',value = result)

def pull(**kwargs):
    ti = kwargs['ti']
    message = ti.xcom_pull(key = 'push_result', task_ids='tarea_1')
    print(message)

############################################################################ Dags ################################################

dag = DAG(
    dag_id="Laboratorio_Jp",
    start_date=days_ago(1),
    schedule_interval='0 23 * * *',
    catchup=False
)

############################################################################ Tasks ################################################

inicio = EmptyOperator(
    task_id="inicio_de_tarea", 
    dag=dag
)
  
branching = BranchPythonOperator(
    task_id='branching',
    python_callable=python_branch,
    provide_context=True,
    dag=dag,
)

tarea_1 = PythonOperator(
    task_id='tarea_1',
    python_callable=push,
    provide_context=True,
    dag=dag
)

tarea_2 = PythonOperator(
    task_id='tarea_2',
    python_callable=pull,
    provide_context=True,
    dag=dag
)

trigger_dag = TriggerDagRunOperator(
    task_id='trigger_dag',
    trigger_dag_id='dag_alternativo',  
    dag=dag,
)

tarea_alterna = EmptyOperator(
    task_id="tarea_alterna", 
    dag=dag
)

end_of_flow = EmptyOperator(
    task_id="end_of_flow", 
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, 
    dag=dag
)


inicio >> branching
branching >> tarea_1 >> tarea_2 >> trigger_dag >> end_of_flow
branching >> tarea_alterna >> end_of_flow