import os
from airflow import DAG

from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME','/opt/airflow')

URL_PREFIX = 'https://data.gharchive.org'
URL_PATH = URL_PREFIX + '/{{ ds }}-{0..23}.json.gz'
OUTPUT_PATH = AIRFLOW_HOME + '/output-{{ ds }}.json.gz'
SCRIPT_PATH = AIRFLOW_HOME +  "/dags/script/spark.py"
# TABLE_FORMAT = 'output_{{ dag_run.logical_date.strftime(\'%Y-%m\') }}'
KEY = 'test/{{ ds }}.json.gz'
SCRIPT_KEY = 'script/spark.py'
BUCKET_NAME = "nerdward-bucket"
# REDSHIFT_TABLE = 'redshift-cluster-1'
SPARK_STEPS = [
    {
        "Name": "Batch Process",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "s3://{{ params.BUCKET_NAME }}/{{ params.SCRIPT_KEY }}",
                "--input",
                "s3://{{ params.BUCKET_NAME }}/test/{{ ds }}.json.gz",
            ],
        },
    },
]

JOB_FLOW_OVERRIDES = {
    "Name": "Github Batch",
    "ReleaseLabel": "emr-6.4.0",
    "Applications": [{"Name": "Spark"}],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm4.xlarge',
                'InstanceCount': 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    'VisibleToAllUsers': True
}

def upload_to_s3(filename, key):
    hook = S3Hook()
    hook.load_file(filename=filename, key=key, bucket_name=BUCKET_NAME, replace=True)

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
            key = KEY
        )
    )

    script_upload_task = PythonOperator(
        task_id= 'Script_To_S3',
        python_callable= upload_to_s3,
        op_kwargs=dict(
            filename = SCRIPT_PATH, 
            key = SCRIPT_KEY
        )
    )

    remove_file = BashOperator(
        task_id= 'Remove_File_from_Local',
        bash_command=f'rm {OUTPUT_PATH}'
    )

    # Create an EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="Create_EMR_Cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        emr_conn_id="emr_default"
    )
        # Add your steps to the EMR cluster
    step_adder = EmrAddStepsOperator(
        task_id="Add_Step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_Cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=SPARK_STEPS,
        params={ # these params are used to fill the paramterized values in SPARK_STEPS json
            "BUCKET_NAME": BUCKET_NAME,
            "SCRIPT_KEY": SCRIPT_KEY,
            "KEY": KEY,
        },
    )

    last_step = len(SPARK_STEPS) - 1 # this value will let the sensor know the last step to watch
    # wait for the steps to complete
    step_checker = EmrStepSensor(
        task_id="watch_step",
        job_flow_id="{{ task_instance.xcom_pull('Create_EMR_Cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='Add_Step', key='return_value')["
        + str(last_step)
        + "] }}",
        aws_conn_id="aws_default",
    )

        # Terminate the EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_Cluster', key='return_value') }}",
        aws_conn_id="aws_default"
    )


    wget_task >> [upload_task ,script_upload_task] >> remove_file
    remove_file >> create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster