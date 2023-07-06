import os
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from datetime import datetime
import pandas as pd
import yfinance as yf

# define variables to be used
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
dataset_file = "YOUR_DATASET_CSV_FILE"
dataset_lst = ["YOUR_DATASET_" + str(i) + ".csv" for i in range(5)]
dataset_name = "YOUR_BQ_DATASET"
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'YOUR_BQ_DATASET')
table_name = "YOUR_BQ_TABLE"

# define python function for used in PythonOperator
def get_top500_companies():
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    tables = pd.read_html(url)
    sp500_table = tables[0]  # Assuming the first table on the page contains the S&P 500 companies

    # Extract the required columns
    sp500_companies = sp500_table['Symbol'].tolist()
    # remove error company ticker
    sp500_companies.remove("BF.B")
    sp500_companies.remove("BRK.B")

    return sp500_companies

def get_all_intraday(inx):
    if inx >= 0 and inx < 5:
        company_lst = get_top500_companies()
        company_lst = company_lst[100 * inx: 100 * (inx + 1)] if inx < 4 else company_lst[100 * inx:]
        data = yf.download(tickers=company_lst[0], period='1m', interval='1m', progress=False)
        for i in range(1, len(company_lst)):
            temp = yf.download(tickers=company_lst[i], period='1m', interval='1m', progress=False)
            temp["ticker"] = company_lst[i]
            data = pd.concat([data, temp], axis = 0)
        data = data.iloc[:, [6, 0, 1, 2, 3, 4, 5]].reset_index()
        data["Datetime"] = data["Datetime"].apply(lambda x: x.to_pydatetime().replace(tzinfo = None))
        data.to_csv(f"{path_to_local_home}/{dataset_lst[inx]}", index = False)
        return
    else:
        print("Error in the index")
        return

def join_all_stocks(dlst, dfile):
    if len(dlst) == 0:
        print("The file list that you provide is empty")
    
    res = pd.read_csv(f"{path_to_local_home}/{dlst[0]}")
    
    if len(dlst) > 1:
        for i in range(1, len(dlst)):
            temp = pd.read_csv(f"{path_to_local_home}/{dlst[i]}")
            res = pd.concat([res, temp])
    
    res.to_csv(f"{path_to_local_home}/{dfile}", index = False)
    return 

# define the default arguments for dags
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,    
    'start_date': datetime(2023, 6, 19),
    'end_date': datetime(2100, 12, 31),
    'email': ['airflow@airflow.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2
}
# define the dags
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="* * * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1
) as dag:
    
    intraday_task_lst = []
    for task_inx in range(5):
        task_name = "get_stock_" + str(task_inx)
        new_task = PythonOperator(
            task_id = task_name,
            python_callable = get_all_intraday,
            op_kwargs = {
                "inx": task_inx
            },
            dag = dag
        )
        intraday_task_lst.append(new_task)
    
    join_all_stocks_task = PythonOperator(
        task_id = "joined_all_stocks",
        python_callable = join_all_stocks,
        op_kwargs = {
            "dlst": dataset_lst,
            "dfile": dataset_file
        },
        dag = dag
    )

    local_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id = "local_to_gcs",
        src = f"{path_to_local_home}/{dataset_file}",
        dst = f"raw/{dataset_file}",
        bucket = BUCKET,
    )
    
    gcs_to_bigquery_task = GCSToBigQueryOperator(
        task_id = "gcs_to_bigquery",
        bucket = BUCKET,
        source_objects = f"raw/{dataset_file}",
        destination_project_dataset_table = f"{dataset_name}.{table_name}",
        schema_fields = [
            {'name': 'Datetime', 'type': 'DATETIME', 'mode': 'NULLABLE'},
            {'name': 'Ticker', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Open', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'High', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'Low', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'Close', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'AdjClose', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'Volume', 'type': 'FLOAT', 'mode': 'NULLABLE'}
        ],
        write_disposition='WRITE_APPEND',
        dag=dag,
    )
    
intraday_task_lst >> join_all_stocks_task >>  local_to_gcs_task >> gcs_to_bigquery_task
