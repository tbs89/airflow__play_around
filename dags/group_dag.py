from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# When using subdags
from airflow.operators.subdag import SubDagOperator 
from sub_dags.subdag_downloads import subdag_downloads
from sub_dags.subdag_transforms import subdag_transforms

# When using task groups
from group_dags.group_downloads import downloads_task
from group_dags.group_transforms import transforms_task




with DAG('group_dag', start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:
    
    args = {
        'start_date': dag.start_date,
        'schedule_interval': dag.schedule_interval,
        'catchup': dag.catchup
    }
    
    
    """
    # When using subdags
    downloads = SubDagOperator(
        task_id='downloads',
        subdag=subdag_downloads(
            dag.dag_id, 'downloads', args
        )
    )
    
    transforms = SubDagOperator(
        task_id='transforms',
        subdag=subdag_transforms(
            dag.dag_id, 'transforms', args
        )
    )"""
    
    # When using task groups
    downloads = downloads_task()
    
    transforms = transforms_task()
    
    
    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )
 
    
 
    downloads >> check_files >> transforms