from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from jobs import bronze_ingest, create_minio_buckets

default_args= {
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1
}

JARS_PATH = "/opt/spark/jars"

with DAG(
    dag_id='stack_overflow_pipeline',
    default_args=default_args,
    description='Deployment of a Lakehouse (bronze, silver, gold layers) for Stack Overflow data using Medallion architecture',
    schedule_interval=None,
    catchup=False
) as dag:
    create_buckets_task = PythonOperator(
        task_id="create_minio_buckets_task",
        python_callable=create_minio_buckets.main
    )
    bronze_task = PythonOperator(
        task_id='bronze_ingest',
        python_callable=bronze_ingest.main
    )

    silver_task = SparkSubmitOperator(
        task_id="silver_transform",
        application="/opt/airflow/jobs/silver_transform.py",
        conn_id="spark_default",
        verbose=True,
        conf={
            "spark.jars": (
                f"{JARS_PATH}/scala-library-2.12.18.jar,"
                f"{JARS_PATH}/hadoop-aws-3.3.4.jar,"
                f"{JARS_PATH}/aws-java-sdk-bundle-1.12.761.jar,"
                f"{JARS_PATH}/iceberg-spark-runtime-3.5_2.12-1.5.0.jar,"
                f"{JARS_PATH}/iceberg-aws-1.5.0.jar,"
                f"{JARS_PATH}/nessie-spark-extensions-3.5_2.12-0.95.0.jar,"
                f"{JARS_PATH}/postgresql-42.7.3.jar"
            ),

            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
            ),

            "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.nessie.uri": "http://nessie:19120/api/v1",
            "spark.sql.catalog.nessie.ref": "main",
            "spark.sql.catalog.nessie.authentication.type": "NONE",
            "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.sql.catalog.nessie.s3.endpoint": "http://minio:9000",
            "spark.sql.catalog.nessie.s3.path-style-access": "true",
            "spark.sql.catalog.nessie.s3.region": "us-east-1",
            "spark.sql.catalog.nessie.warehouse": "s3a://silver/",

            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "admin",
            "spark.hadoop.fs.s3a.secret.key": "password",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.region": "us-east-1",

            "spark.hadoop.fs.s3a.connection.timeout": "60000",
            "spark.hadoop.fs.s3a.connection.establish.timeout": "60000",
            "spark.hadoop.fs.s3a.connection.request.timeout": "60000",
            "spark.hadoop.fs.s3a.connection.socket.timeout": "60000",
            "spark.hadoop.fs.s3a.connection.maximum": "50",
            "spark.hadoop.fs.s3a.attempts.maximum": "20",
            "spark.hadoop.fs.s3a.retry.limit": "5",

            "spark.master": "spark://spark-master:7077",
            "spark.submit.deployMode": "client",
            "spark.hadoop.aws.region": "us-east-1",
            "spark.driver.extraJavaOptions": "-Daws.region=us-east-1",
            "spark.executor.extraJavaOptions": "-Daws.region=us-east-1",
        },
        env_vars={
            "AWS_REGION": "us-east-1",
            "AWS_DEFAULT_REGION": "us-east-1",
        },
    )

    gold_task = SparkSubmitOperator(
        task_id="gold_agg",
        application="/opt/airflow/jobs/gold_agg.py",
        conn_id="spark_default",
        verbose=True,
        conf={
            "spark.jars": (
                f"{JARS_PATH}/scala-library-2.12.18.jar,"
                f"{JARS_PATH}/hadoop-aws-3.3.4.jar,"
                f"{JARS_PATH}/aws-java-sdk-bundle-1.12.761.jar,"
                f"{JARS_PATH}/iceberg-spark-runtime-3.5_2.12-1.5.0.jar,"
                f"{JARS_PATH}/iceberg-aws-1.5.0.jar,"
                f"{JARS_PATH}/nessie-spark-extensions-3.5_2.12-0.95.0.jar,"
                f"{JARS_PATH}/postgresql-42.7.3.jar"
            ),

            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
            ),

            "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.nessie.uri": "http://nessie:19120/api/v1",
            "spark.sql.catalog.nessie.ref": "main",
            "spark.sql.catalog.nessie.authentication.type": "NONE",
            "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.sql.catalog.nessie.s3.endpoint": "http://minio:9000",
            "spark.sql.catalog.nessie.s3.path-style-access": "true",
            "spark.sql.catalog.nessie.s3.region": "us-east-1",
            "spark.sql.catalog.nessie.warehouse": "s3a://gold/",

            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "admin",
            "spark.hadoop.fs.s3a.secret.key": "password",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.region": "us-east-1",

            "spark.hadoop.fs.s3a.connection.timeout": "60000",
            "spark.hadoop.fs.s3a.connection.establish.timeout": "60000",
            "spark.hadoop.fs.s3a.connection.request.timeout": "60000",
            "spark.hadoop.fs.s3a.connection.socket.timeout": "60000",
            "spark.hadoop.fs.s3a.connection.maximum": "50",
            "spark.hadoop.fs.s3a.attempts.maximum": "20",
            "spark.hadoop.fs.s3a.retry.limit": "5",

            "spark.master": "spark://spark-master:7077",
            "spark.submit.deployMode": "client",
            "spark.hadoop.aws.region": "us-east-1",
            "spark.driver.extraJavaOptions": "-Daws.region=us-east-1",
            "spark.executor.extraJavaOptions": "-Daws.region=us-east-1",
        },
        env_vars={
            "AWS_REGION": "us-east-1",
            "AWS_DEFAULT_REGION": "us-east-1",
        },
    )

create_buckets_task >> bronze_task >> silver_task >> gold_task