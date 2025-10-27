"""
Airflow DAG для чтения Iceberg таблиц через Spark + Hive

Использует:
- Airflow Spark Connector для подключения к Spark Master
- Airflow AWS Connector для подключения к S3
- Kubernetes Secret для Hive certificate (.jceks файл)
"""

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

# =============================================================================
# НАСТРОЙКИ - Через Airflow Variables или в коде
# =============================================================================

# Константы
SPARK_CONN_ID = "spark_default"  # Имя коннектора Spark в Airflow
K8S_NAMESPACE = "airflow"  # Namespace где находится Airflow
HIVE_CERT_SECRET_NAME = "hive-jceks-secret"  # Имя Kubernetes secret


# Настройки DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Создаем DAG
dag = DAG(
    'iceberg_read_via_hive',
    default_args=default_args,
    description='Чтение Iceberg таблиц через Hive',
    schedule_interval=timedelta(hours=6),  # Каждые 6 часов
    catchup=False,
    tags=['iceberg', 'spark', 'hive', 's3'],
)


# Task 1: Подготовка среды (монтаж Hive certificate из K8s secret)
prepare_task = KubernetesPodOperator(
    task_id='prepare_hive_cert',
    namespace=K8S_NAMESPACE,
    name='prepare-hive-cert',
    image='busybox:latest',
    cmds=['sh', '-c'],
    arguments=[
        '''
        echo "Монтируем Hive certificate из K8s secret"
        echo "Cert будет доступен в /tmp/hive.jceks"
        '''
    ],
    volume_mounts=[
        {
            'name': 'hive-cert',
            'mountPath': '/tmp/hive.jceks',
            'subPath': 'hive.jceks',
            'readOnly': True
        }
    ],
    volumes=[
        {
            'name': 'hive-cert',
            'secret': {
                'secretName': HIVE_CERT_SECRET_NAME
            }
        }
    ],
    dag=dag,
)


# Task 2: Чтение Iceberg таблицы
# Важно: Настройте Airflow Variables перед запуском:
# - ICEBERG_BUCKET, ICEBERG_SCHEMA, ICEBERG_TABLE
# - hive_metastore_uri, s3_endpoint
# - aws_access_key_id, aws_secret_access_key
# Job скрипт сам читает переменные из Airflow Variables через os.getenv()
read_iceberg_task = SparkSubmitOperator(
    task_id='read_iceberg_table',
    application="s3a://more/spark/scripts/iceberg_read_job.py",
    conn_id=SPARK_CONN_ID,
    name="iceberg_read_job",
    dag=dag
)


# Зависимости
prepare_task >> read_iceberg_task

