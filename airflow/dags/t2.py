from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessCreateApplicationOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessDeleteApplicationOperator
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessJobSensor
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessApplicationSensor

JOB_ROLE_ARN = "arn:aws:iam::XXXXXXXXX:role/AmazonEMR-ExecutionRole-1700757232948"
S3_LOGS_BUCKET = "bucket-organizations"

DEFAULT_MONITORING_CONFIG = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {"logUri": f"s3://bucket-organizations/logs/"}
    },
}

networkConfiguration={
        'subnetIds': [
            'subnet-054da37447e4ac846',
            'subnet-0fdd57e4e5a85d87d',
        ],
        'securityGroupIds': [
            'sg-049d2081697556c8b',
        ]
    }

with DAG(
    dag_id="emr_serverless_job",
    schedule_interval=None,
    start_date=datetime(2023, 12, 7),
    tags=["final_version"],
    catchup=False,
) as dag:
    create_app = EmrServerlessCreateApplicationOperator(
        task_id="create_spark_app",
        job_type="SPARK",
        release_label="emr-6.15.0",
        aws_conn_id= 'aws_connect',
        config={"name": "airflow-test", "networkConfiguration": networkConfiguration},
    )

    create_app.wait_for_completion = True
    application_id = create_app.output

    sensor_app = EmrServerlessApplicationSensor(
        task_id="sensor_app",
        application_id=application_id,
        aws_conn_id= 'aws_connect',
    )   

    job1 = EmrServerlessStartJobOperator(
        task_id="start_job_1",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        aws_conn_id= 'aws_connect',
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://bucket-organizations/scripts/unzip_and_process.py",
                "sparkSubmitParameters": "--conf spark.archives=s3://bucket-organizations/scripts/pyspark_python_extra_libs.tar.gz#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    sensor_job = EmrServerlessJobSensor(
        task_id="sensor_job",
        application_id=application_id,
        job_run_id=job1.output,
        aws_conn_id= 'aws_connect',
        target_states={"SUCCESS"},
    )   

    delete_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_app",
        application_id=application_id,
        aws_conn_id= 'aws_connect',
        trigger_rule="all_done",
    )

    create_app >> sensor_app >> job1 >> sensor_job >> delete_app