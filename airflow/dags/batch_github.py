import os
from airflow import DAG

from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME','/opt/airflow')

URL_PREFIX = 'https://data.gharchive.org'
URL_PATH = URL_PREFIX + '/{{ ds }}-{0..23}.json.gz'
OUTPUT_PATH = AIRFLOW_HOME + '/output-{{ ds }}.json.gz'
SCRIPT_PATH = AIRFLOW_HOME +  "/dags/script/spark.py"
# TABLE_FORMAT = 'output_{{ dag_run.logical_date.strftime(\'%Y-%m\') }}'
KEY = 'test/{{ ds }}.json.gz'
SCRIPT_KEY = './script/spark.py'
BUCKET_NAME = "nerdward-bucket"
# REDSHIFT_TABLE = 'redshift-cluster-1'

def upload_to_s3(filename, key, bucket_name):
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

upload_s3 = DAG(
    'Batch_Github_Archives',
    schedule_interval= '0 6 2 * *',
    start_date= datetime(2022, 5, 1),
    end_date = datetime(2022, 5, 31)
)

with upload_s3:
    wget_task = BashOperator(
        task_id = 'DownloadToLocal',
        bash_command= f"curl -sSLf {URL_PATH} > {OUTPUT_PATH}"
    )

    upload_task = PythonOperator(
        task_id= 'Upload_To_S3',
        python_callable= upload_to_s3,
        op_kwargs=dict(
            filename = OUTPUT_PATH, 
            key = KEY, 
            bucket_name = BUCKET_NAME
        )
    )

    script_upload_task = PythonOperator(
        task_id= 'Script_To_S3',
        python_callable= upload_to_s3,
        op_kwargs=dict(
            filename = SCRIPT_PATH, 
            key = SCRIPT_KEY, 
            bucket_name = BUCKET_NAME
        )
    )

    remove_file = BashOperator(
        task_id= 'Remove_File_from_Local',
        bash_command=f'rm {OUTPUT_PATH}'
    )


    wget_task >> [upload_task ,script_upload_task]