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
SCRIPT_PATH = "./dags/script/git_batch_script.py"
# TABLE_FORMAT = 'output_{{ dag_run.logical_date.strftime(\'%Y-%m\') }}'
KEY = 'test/{{ ds }}.json.gz'
SCRIPT_KEY = 'script/git_batch_script.py'
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
        # bash_command = 'echo "{{ ds }} {{ dag_run.logical_date.strftime(\'%Y-%m\') }} "'
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

#     create_table = RedshiftSQLOperator(
#         task_id='create_table',
#         sql= f"""CREATE TABLE IF NOT EXISTS "{TABLE_FORMAT}" (
#                 "VendorID" BIGINT, 
#                 tpep_pickup_datetime TIMESTAMP WITHOUT TIME ZONE, 
#                 tpep_dropoff_datetime TIMESTAMP WITHOUT TIME ZONE, 
#                 passenger_count FLOAT(53), 
#                 trip_distance FLOAT(53), 
#                 "RatecodeID" FLOAT(53), 
#                 store_and_fwd_flag TEXT, 
#                 "PULocationID" BIGINT, 
#                 "DOLocationID" BIGINT, 
#                 payment_type BIGINT, 
#                 fare_amount FLOAT(53), 
#                 extra FLOAT(53), 
#                 mta_tax FLOAT(53), 
#                 tip_amount FLOAT(53), 
#                 tolls_amount FLOAT(53), 
#                 improvement_surcharge FLOAT(53), 
#                 total_amount FLOAT(53), 
#                 congestion_surcharge FLOAT(53), 
#                 airport_fee FLOAT(53)
#             );
#             """
#     )

#     insert_to_rds = S3ToRedshiftOperator(
#         s3_bucket=BUCKET_NAME,
#         s3_key=KEY,
#         schema='"public"',
#         table=f'"{TABLE_FORMAT}"',
#         copy_options=['parquet'],
#         task_id='transfer_s3_to_redshift',
#         aws_conn_id='s3_conn'
# )

    wget_task >> upload_task 
    # >> create_table >> insert_to_rds