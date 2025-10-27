"""
Universal Airflow DAG для чтения Iceberg таблиц из Hive
и записи в целевые БД (PostgreSQL, Greenplum, Impala, Oracle)

Управление активными задачами через Airflow Variables:
- ENABLE_POSTGRESQL = "true"/"false"
- ENABLE_GREENPLUM = "true"/"false"
- ENABLE_IMPALA = "true"/"false"
- ENABLE_ORACLE = "true"/"false"

По умолчанию активны только PostgreSQL и Greenplum
"""

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.models import Variable
from datetime import datetime, timedelta


SPARK_CONN_ID = "spark_default"
K8S_NAMESPACE = "airflow"
HIVE_CERT_SECRET_NAME = "hive-jceks-secret"


def should_run_task(**context):
    """Проверяет нужно ли выполнять task на основе Airflow Variable"""
    task_name = context['task'].task_id
    
    if 'postgresql' in task_name:
        flag = Variable.get("ENABLE_POSTGRESQL", default_var="true")
    elif 'greenplum' in task_name:
        flag = Variable.get("ENABLE_GREENPLUM", default_var="true")
    elif 'impala' in task_name:
        flag = Variable.get("ENABLE_IMPALA", default_var="false")
    elif 'oracle' in task_name:
        flag = Variable.get("ENABLE_ORACLE", default_var="false")
    else:
        return True
    
    return flag.lower() in ("true", "1", "yes", "on")


default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'iceberg_universal_pipeline',
    default_args=default_args,
    description='Universal Iceberg to Target DBs pipeline',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['iceberg', 'spark', 'hive', 'postgresql', 'greenplum', 'impala', 'oracle'],
)


prepare_task = KubernetesPodOperator(
    task_id='prepare_hive_cert',
    namespace=K8S_NAMESPACE,
    name='prepare-hive-cert',
    image='busybox:latest',
    cmds=['sh', '-c'],
    arguments=['echo "Mounting Hive certificate from K8s secret"'],
    volume_mounts=[{
        'name': 'hive-cert',
        'mountPath': '/tmp/hive.jceks',
        'subPath': 'hive.jceks',
        'readOnly': True
    }],
    volumes=[{
        'name': 'hive-cert',
        'secret': {'secretName': HIVE_CERT_SECRET_NAME}
    }],
    dag=dag,
)


read_iceberg_task = SparkSubmitOperator(
    task_id='read_iceberg_from_hive',
    application="s3a://more/spark/scripts/iceberg_read_job.py",
    conn_id=SPARK_CONN_ID,
    name="iceberg_read_hive",
    dag=dag
)


check_postgresql = ShortCircuitOperator(
    task_id='check_enable_postgresql',
    python_callable=should_run_task,
    dag=dag
)

write_postgresql = SparkSubmitOperator(
    task_id='write_to_postgresql',
    application="s3a://more/spark/scripts/iceberg_to_postgresql_job.py",
    conn_id=SPARK_CONN_ID,
    name="iceberg_to_postgresql",
    dag=dag
)


check_greenplum = ShortCircuitOperator(
    task_id='check_enable_greenplum',
    python_callable=should_run_task,
    dag=dag
)

write_greenplum = SparkSubmitOperator(
    task_id='write_to_greenplum',
    application="s3a://more/spark/scripts/iceberg_to_greenplum_job.py",
    conn_id=SPARK_CONN_ID,
    name="iceberg_to_greenplum",
    dag=dag
)


# check_impala = ShortCircuitOperator(
#     task_id='check_enable_impala',
#     python_callable=should_run_task,
#     dag=dag
# )

# write_impala = SparkSubmitOperator(
#     task_id='write_to_impala',
#     application="s3a://more/spark/scripts/iceberg_to_impala_job.py",
#     conn_id=SPARK_CONN_ID,
#     name="iceberg_to_impala",
#     dag=dag
# )


# check_oracle = ShortCircuitOperator(
#     task_id='check_enable_oracle',
#     python_callable=should_run_task,
#     dag=dag
# )

# write_oracle = SparkSubmitOperator(
#     task_id='write_to_oracle',
#     application="s3a://more/spark/scripts/iceberg_to_oracle_job.py",
#     conn_id=SPARK_CONN_ID,
#     name="iceberg_to_oracle",
#     dag=dag
# )


# Dependencies
prepare_task >> read_iceberg_task
read_iceberg_task >> check_postgresql >> write_postgresql
read_iceberg_task >> check_greenplum >> write_greenplum
# read_iceberg_task >> check_impala >> write_impala
# read_iceberg_task >> check_oracle >> write_oracle

