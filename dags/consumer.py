from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime


# DAG that interacts with a Dataset (Consumer)

my_file = Dataset("/tmp/my_file.txt")
my_file_2 = Dataset("/tmp/my_file_2.txt")


with DAG(
    dag_id="consumer",
    schedule=[my_file, my_file_2], # As soon as the file is updated, DAG will run
    start_date=datetime(2022, 1, 1),
    catchup=False
         
):
    
    @task
    def read_dataset():
        with open(my_file.uri, "r") as f:
            print(f.read())

    @task
    def read_dataset_2():
        with open(my_file_2.uri, "r") as f:
            print(f.read())
            
    read_dataset() >> read_dataset_2()
    
    
    
    