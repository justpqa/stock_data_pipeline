from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime, date

# define some functions for using
def hello_world():
    print("Hello world")
    
def get_time():
    time = datetime.now().strftime("%H:%M:%S")
    date = datetime.today()
    print("Now is {}".format(time))
    print("Today is {}".format(date))
    
# create a DAG object
with DAG(dag_id = "first_dag",
         start_date = datetime(2023, 6, 18),
         schedule_interval="@hourly",
         catchup=False) as dag:
    # now we start making the task
    task1 = PythonOperator(task_id = "Hello_world",
                           python_callable = hello_world)
    
    task2 = PythonOperator(task_id = "Print_time",
                           python_callable = get_time)
    
task1 >> task2