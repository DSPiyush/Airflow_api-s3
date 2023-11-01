from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

import pandas as pd

dag = DAG(dag_id = 'File_Creation', 
          description = 'To create a file',
          start_date = datetime(2023, 4, 28), 
          schedule_interval = "@daily", 
          catchup = False        
          )


def check_response(res):
    print(f"Response Status : {res.status_code}")
    return True


task_sense_api = HttpSensor(task_id = 'check_http_response', 
                             http_conn_id = "gutendex",
                             endpoint = f"/books/?page={Variable.get(key = 'page_num')}",
                             response_check = lambda response:check_response(response),
                             dag = dag)

file_name = 'result.json'
def get_api_data(res):
    json_data = res.json() 
    # This is a dictionary which has one of the keys as result, which indeed is a list of dictionaries
    list_id = [item['id'] for item in json_data['results']]
    list_title = [item['title'] for item in json_data['results']]
    list_download_count = [item['download_count'] for item in json_data['results']]
    df = pd.DataFrame(data = {'id' : list_id, 
                              'title' : list_title, 
                              'download_count': list_download_count}
                      )

    df.to_csv('/opt/airflow/dags/data.csv',sep = ',',index =False)

        

task_get_data_api = SimpleHttpOperator(task_id = 'get_http_data', 
                                       http_conn_id = "gutendex",
                                       endpoint = f"/books/?page={Variable.get(key = 'page_num')}",
                                       method = 'GET',
                                       response_filter = lambda response:get_api_data(response),
                                       dag = dag)


def s3_upload(file_name, key, bucket_name):
    hook = S3Hook('s3con')
    hook.load_file(filename = file_name, key = key, bucket_name = bucket_name)
    return True    



task_upload_data_s3 = PythonOperator(task_id = 'upload_data_s3_from_local', 
                                     python_callable = s3_upload,
                                     op_kwargs = {'file_name': "/opt/airflow/dags/data.csv",
                                                  'key' : f"data{Variable.get(key = 'page_num')}.csv",
                                                  'bucket_name' : 'aws-airflow-file'},
                                     dag = dag)

def increment_page_number():
    Variable.set(key = 'page_num', value = int(Variable.get(key = 'page_num')) + 1)
    print(f"page_num Variable value is now : {Variable.get(key = 'page_num')}")

task_increment_page_number_forNextDagRun =  PythonOperator(task_id = "task_increment_page_number_forNextDagRun", 
                                                           python_callable = increment_page_number,
                                                           dag = dag )

task_sense_api>>task_get_data_api>>task_upload_data_s3>>task_increment_page_number_forNextDagRun



