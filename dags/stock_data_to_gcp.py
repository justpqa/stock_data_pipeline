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
    sp500_companies.remove("BF.B")
    sp500_companies.remove("BRK.B")

    return sp500_companies

def get_top500_intraday():
    # get the list of top 500 companies
    company_lst = get_top500_companies()
    # get the intraday data of these companies
    data = yf.download(tickers=company_lst[0], period='1d', interval='60m', progress=False)
    data["ticker"] = company_lst[0]
    for i in range(1, len(company_lst)):
        temp = yf.download(tickers=company_lst[i], period='1d', interval='60m', progress=False)
        temp["ticker"] = company_lst[i]
        data = pd.concat([data, temp], axis = 0)
    data = data.iloc[:, [6, 0, 1, 2, 3, 4, 5]].reset_index()
    data.to_csv(f"{path_to_local_home}/{dataset_file}", index = False)
    
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
    schedule_interval="*/5 * * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1
) as dag:
    
    get_stock_task = PythonOperator(
        task_id = "get_stock",
        python_callable = get_top500_intraday
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
    
get_stock_task >>  local_to_gcs_task >> gcs_to_bigquery_task
