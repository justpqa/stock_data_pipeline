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
from tqdm import tqdm

import pyarrow.csv as pv
import pyarrow.parquet as pq

# define variables to be used
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
dataset_file = "YOUR_DATASET_CSV_FILE"
dataset_name = "YOUR_BQ_DATASET"
#parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'YOUR_BQ_DATASET')
table_name = "YOUR_BQ_TABLE"

# define python function for used in PythonOperator
def get_top500_companies():
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    tables = pd.read_html(url)
    sp500_table = tables[0]  # Assuming the first table on the page contains the S&P 500 companies

    # Extract the required columns
    sp500_companies = sp500_table['Symbol'].tolist()

    return sp500_companies

def get_top500_intraday():
    # get the list of top 500 companies
    company_lst = get_top500_companies()
    # remove error company ticker
    company_lst.remove("BF.B")
    company_lst.remove("BRK.B")
    # get the intraday data of these companies
    data = yf.download(tickers=company_lst[0], period='1d', interval='60m', progress=False)
    data["ticker"] = company_lst[0]
    for i in tqdm(range(1, len(company_lst))):
        temp = yf.download(tickers=company_lst[i], period='1d', interval='60m', progress=False)
        temp["ticker"] = company_lst[i]
        data = pd.concat([data, temp], axis = 0)
    data = data.iloc[:, [6, 0, 1, 2, 3, 4, 5]].reset_index()
    data.to_csv(f"{path_to_local_home}/{dataset_file}", index = False)

# try to convert to parquet later
# def format_to_parquet(src_file):
#     if not src_file.endswith('.csv'):
#         logging.error("Can only accept source files in CSV format, for the moment")
#         return
#     table = pv.read_csv(src_file)
#     pq.write_table(table, src_file.replace('.csv', '.parquet'))


# # NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
# def upload_to_gcs(bucket, object_name, local_file):
#     """
#     Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
#     :param bucket: GCS bucket name
#     :param object_name: target path & file-name
#     :param local_file: source path & file-name
#     :return:
#     """
#     # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
#     # (Ref: https://github.com/googleapis/python-storage/issues/74)
#     storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
#     storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
#     # End of Workaround

#     client = storage.Client()
#     bucket = client.bucket(bucket)

#     blob = bucket.blob(object_name)
#     blob.upload_from_filename(local_file)
    
# define the default arguments for dags
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,    
    'start_date': datetime(2023, 6, 19),
    'end_date': datetime(2023, 6, 26),
    'email': ['airflow@airflow.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2
}
# define the dags
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@hourly",
    default_args=default_args,
    catchup=False,
    max_active_runs=1
) as dag:
    
    get_stock_task = PythonOperator(
        task_id = "get_stock",
        python_callable = get_top500_intraday
    )
    
    # format_to_parquet_task = PythonOperator(
    #     task_id="format_to_parquet_task",
    #     python_callable=format_to_parquet,
    #     op_kwargs={
    #         "src_file": f"{path_to_local_home}/{dataset_file}",
    #     }
    # )

    # local_to_gcs_task = PythonOperator(
    #     task_id="local_to_gcs_task",
    #     python_callable=upload_to_gcs,
    #     op_kwargs={
    #         "bucket": BUCKET,
    #         "object_name": f"raw/{parquet_file}",
    #         "local_file": f"{path_to_local_home}/{parquet_file}",
    #     },
    # )
    
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
        write_disposition='WRITE_TRUNCATE',
        dag=dag,
    )
    
get_stock_task >>  local_to_gcs_task >> gcs_to_bigquery_task