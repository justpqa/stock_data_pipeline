import os
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from selenium.webdriver import ChromeOptions, Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import pandas as pd
import numpy as np
from datetime import datetime
import time

# define variables to be used
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
dataset_file = "YOUR_DATASET_CSV_FILE"
dataset_lst = ["news_data_" + str(i) + ".csv" for i in range(5)]
dataset_name = "YOUR_BQ_DATASET"
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'YOUR_BQ_DATASET')
table_name = "YOUR_BQ_TABLE"

def get_top500_companies():
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    tables = pd.read_html(url)
    sp500_table = tables[0]  # Assuming the first table on the page contains the S&P 500 companies

    # Extract the required columns
    sp500_companies = sp500_table['Symbol'].tolist()
    sp500_companies.remove("BF.B")
    sp500_companies.remove("BRK.B")
    return sp500_companies

def get_news(ticker):
    news_df = pd.DataFrame(columns=["Date", "Ticker", "Title"])
    browser_options = ChromeOptions()
    browser_options.add_argument("--no-sandbox")
    browser_options.add_argument("--headless")
    browser_options.add_argument("--disable-dev-shm-usage")
    driver = Chrome(options = browser_options)
    driver.get("https://www.cnbc.com/quotes/{}?tab=news".format(ticker))
    ticker = ticker
    try:
        for j in range(1, 6):
            try:
                r = driver.find_element(By.XPATH, "/html/body/div[2]/div/div[1]/div[3]/div/div[2]/div[1]/div[5]/div[2]/div/div[1]/ul/li[{}]/div/div".format(j))
                temp = str(r.text).split("\n")
                news_df.loc[len(news_df), news_df.columns] = datetime.strptime(temp[1], "%B %d, %Y"), ticker, temp[0]
            except:
                continue
    except:
        print("Fail in getting news about {}".format(ticker))
        print("")
    
    driver.close()
    driver.quit()
    
    return news_df

def get_all_news(inx):
    if inx >= 0 and inx < 5:
        company_lst = get_top500_companies()
        results_df = pd.DataFrame(columns = ["Date", "Ticker", "Title"])
        company_lst = company_lst[100 * inx: 100 * (inx + 1)] if inx < 4 else company_lst[100 * inx:]
        for c in company_lst:
            temp = get_news(c)
            results_df = pd.concat([results_df, temp])
            print(c)
        results_df = results_df.reset_index().drop("index", axis = 1)
        results_df.to_csv(f"{path_to_local_home}/{dataset_lst[inx]}", index = False)
        return
    else:
        print("Error in the index")
        return

def join_all_news(dlst, dfile):
    if len(dlst) == 0:
        print("The file list that you provide is empty")
    
    res = pd.read_csv(f"{path_to_local_home}/{dlst[0]}")
    
    if len(dlst) > 1:
        for i in range(1, len(dlst)):
            temp = pd.read_csv(f"{path_to_local_home}/{dlst[i]}")
            res = pd.concat([res, temp])
    
    res.to_csv(f"{path_to_local_home}/{dfile}", index = False)
    return 

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,    
    'start_date': datetime(2023, 6, 25),
    'end_date': datetime(2023, 7, 7),
    'email': ['airflow@airflow.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2
}
# define the dags
with DAG(
    dag_id="news_data_ingestion_dag",
    schedule_interval="* *\1 * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1
) as dag:
    
    scraping_task_lst = []
    for task_inx in range(5):
        task_name = "get_all_news_" + str(task_inx)
        new_task = PythonOperator(
            task_id = task_name,
            python_callable = get_all_news,
            op_kwargs = {
                "inx": task_inx
            },
            dag = dag
        )
        scraping_task_lst.append(new_task)
    
    join_all_news_task = PythonOperator(
        task_id = "joined_all_new",
        python_callable = join_all_news,
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
        bucket = BUCKET
    )
    
    gcs_to_bigquery_task = GCSToBigQueryOperator(
        task_id = "gcs_to_bigquery",
        bucket = BUCKET,
        source_objects = f"raw/{dataset_file}",
        destination_project_dataset_table = f"{dataset_name}.{table_name}",
        schema_fields = [
            {'name': 'Date', 'type': 'DATETIME', 'mode': 'NULLABLE'},
            {'name': 'Ticker', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Title', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        write_disposition='WRITE_APPEND',
        dag=dag,
    )
    
scraping_task_lst >> join_all_news_task >> local_to_gcs_task >> gcs_to_bigquery_task
