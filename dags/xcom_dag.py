from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
 
from datetime import datetime
 
def _t1(ti):
    
    result = 20 + 22
    ti.xcom_push(key='number', value=result)
 
def _t2(ti):
    result = ti.xcom_pull(key='number')
    result_2 = result + 2
    ti.xcom_push(key='number_2', value=result_2)
    
    
def _t3(ti):
    result = ti.xcom_pull(key='number_2')
    result_doubled = result * 4
    ti.xcom_push(key='result_doubled', value=result_doubled)
    print(f'The result is {result_doubled}')
    return result_doubled



def _branch(ti):
    value = ti.xcom_pull(key='result_doubled', task_ids='t1')
    if result > 100:
        return 't3'
    else:
        return 't2'

with DAG("xcom_dag", start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:
 
    t1 = PythonOperator(
        task_id='t1',
        python_callable=_t1
    )
    
    branch = BranchPythonOperator(
        task_id='branch',
        python_callable=_branch
    )
 
    t2 = PythonOperator(
        task_id='t2',
        python_callable=_t2
    )
    
    t3 = PythonOperator(
        task_id='t3',
        python_callable=_t3
    )
 
    t1 >> branch >> [t2, t3]
    
    
    
    