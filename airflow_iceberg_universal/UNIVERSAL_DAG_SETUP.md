# Универсальный Iceberg DAG - Руководство по настройке

## Обзор

Универсальный DAG для чтения Iceberg таблиц из Hive и записи в целевые БД:
- PostgreSQL
- Greenplum
- Impala
- Oracle

## Структура файлов

```
iceberg_universal_dag.py          # DAG файл
iceberg_to_postgresql_job.py      # Spark job для PostgreSQL
iceberg_to_greenplum_job.py       # Spark job для Greenplum
iceberg_to_impala_job.py          # Spark job для Impala (закомментирован)
iceberg_to_oracle_job.py          # Spark job для Oracle (закомментирован)
```

## Настройка Airflow Variables

### Общие настройки Iceberg

```python
ICEBERG_BUCKET = "core"
ICEBERG_SCHEMA = "core_stage"
ICEBERG_TABLE = "source_system"
hive_metastore_uri = "thrift://hive-0.hive-svc.k-mgp-v01-mgp-nova-impala.svc.cluster.app:9084"
s3_endpoint = "http://minio.minio:9000"
aws_access_key_id = "your_access_key"
aws_secret_access_key = "your_secret_key"
```

### Режим записи

```python
WRITE_MODE = "append"  # или "overwrite"
```

### Флаги включения задач

```python
ENABLE_POSTGRESQL = "true"   # включить PostgreSQL
ENABLE_GREENPLUM = "true"    # включить Greenplum
ENABLE_IMPALA = "false"       # выключить Impala
ENABLE_ORACLE = "false"       # выключить Oracle
```

### PostgreSQL настройки

```python
PG_CONN_ID = "postgres_default"
TARGET_PG_SCHEMA = "public"
TARGET_PG_TABLE = "iceberg_data"
```

### Greenplum настройки

```python
GP_CONN_ID = "greenplum_default"
TARGET_GP_SCHEMA = "public"
TARGET_GP_TABLE = "iceberg_data"
```

### Impala настройки (для будущего использования)

```python
IMPALA_CONN_ID = "impala_default"
IMPALA_TARGET_SCHEMA = "analytics"
TARGET_IMPALA_TABLE = "iceberg_data"
```

### Oracle настройки (для будущего использования)

```python
ORACLE_CONN_ID = "oracle_default"
TARGET_ORACLE_SCHEMA = "DATA_SCHEMA"
TARGET_ORACLE_TABLE = "ICEBERG_DATA"
```

## Настройка Airflow Connections

### PostgreSQL Connection

```python
from airflow.models import Connection
from airflow.settings import Session

conn = Connection(
    conn_id='postgres_default',
    conn_type='postgres',
    host='postgres-host',
    port=5432,
    login='username',
    password='password',
    schema='database_name'
)
session = Session()
session.add(conn)
session.commit()
```

### Greenplum Connection

```python
conn = Connection(
    conn_id='greenplum_default',
    conn_type='postgres',
    host='greenplum-host',
    port=5432,
    login='username',
    password='password',
    schema='database_name'
)
session = Session()
session.add(conn)
session.commit()
```

### Impala Connection (для будущего)

```python
conn = Connection(
    conn_id='impala_default',
    conn_type='impala',
    host='impala-host',
    port=21050,
    login='username',
    password='password',
    schema='database_name'
)
```

### Oracle Connection (для будущего)

```python
conn = Connection(
    conn_id='oracle_default',
    conn_type='oracle',
    host='oracle-host',
    port=1521,
    login='username',
    password='password',
    schema='database_name'
)
```

## Загрузка файлов

### 1. Загрузить DAG в Airflow

```bash
cp iceberg_universal_dag.py /path/to/airflow/dags/
```

### 2. Загрузить Spark jobs в S3

```bash
aws s3 cp iceberg_to_postgresql_job.py s3://more/spark/scripts/
aws s3 cp iceberg_to_greenplum_job.py s3://more/spark/scripts/
aws s3 cp iceberg_to_impala_job.py s3://more/spark/scripts/
aws s3 cp iceberg_to_oracle_job.py s3://more/spark/scripts/
```

### 3. Создать Kubernetes Secret для Hive certificate

```bash
kubectl create secret generic hive-jceks-secret \
  --from-file=hive.jceks=${HOME}/hive.jceks \
  -n airflow
```

## Запуск DAG

### Через UI

1. Откройте Airflow UI
2. Найдите DAG `iceberg_universal_pipeline`
3. Включите его (toggle)
4. Нажмите "Trigger DAG"

### Через CLI

```bash
airflow dags trigger iceberg_universal_pipeline
```

## Управление активными задачами

### Включить/выключить конкретную задачу

Через Airflow UI: Admin → Variables

Изменить значение:
- `ENABLE_POSTGRESQL` = "false" (выключить)
- `ENABLE_GREENPLUM` = "true" (включить)

### Включить Impala или Oracle (когда понадобится)

1. Раскомментировать соответствующие секции в DAG:

```python
check_impala = ShortCircuitOperator(
    task_id='check_enable_impala',
    python_callable=should_run_task,
    dag=dag
)

write_impala = SparkSubmitOperator(
    task_id='write_to_impala',
    application="s3a://more/spark/scripts/iceberg_to_impala_job.py",
    conn_id=SPARK_CONN_ID,
    name="iceberg_to_impala",
    dag=dag
)
```

2. Добавить зависимость:

```python
read_iceberg_task >> check_impala >> write_impala
```

3. Настроить Variables и Connection для Impala

## Мониторинг

### Просмотр логов

```bash
# Логи чтения Iceberg
airflow tasks logs iceberg_universal_pipeline read_iceberg_from_hive {dag_run_id} {task_instance_id}

# Логи записи в PostgreSQL
airflow tasks logs iceberg_universal_pipeline write_to_postgresql {dag_run_id} {task_instance_id}

# Логи записи в Greenplum
airflow tasks logs iceberg_universal_pipeline write_to_greenplum {dag_run_id} {task_instance_id}
```

## Troubleshooting

### Ошибка подключения к Hive

```
org.apache.hadoop.hive.ql.metadata.HiveException
```

Решение:
- Проверьте `hive_metastore_uri` в Variables
- Проверьте доступность Hive в кластере
- Проверьте Kubernetes Secret `hive-jceks-secret`

### Ошибка подключения к PostgreSQL/Greenplum

```
java.sql.SQLException
```

Решение:
- Проверьте Airflow Connection параметры
- Проверьте доступность БД
- Проверьте credentials в Connection

### Задача не выполняется (ShortCircuit)

Решение:
- Проверьте значение флага в Variables (должно быть "true")
- Проверьте логи check_* задачи

## Примеры использования

### Пример 1: Запись только в PostgreSQL

```python
# В Variables:
ENABLE_POSTGRESQL = "true"
ENABLE_GREENPLUM = "false"
ENABLE_IMPALA = "false"
ENABLE_ORACLE = "false"
```

### Пример 2: Запись в PostgreSQL и Greenplum (по умолчанию)

```python
# В Variables:
ENABLE_POSTGRESQL = "true"
ENABLE_GREENPLUM = "true"
```

### Пример 3: Добавить Impala

1. Раскомментировать код в DAG
2. Загрузить `iceberg_to_impala_job.py` в S3
3. Настроить Variables и Connection
4. Установить `ENABLE_IMPALA = "true"`

## Расширение функциональности

### Добавление новых целевых БД

1. Создать новый Spark job: `iceberg_to_{db}_job.py`
2. Добавить в DAG:
   - ShortCircuitOperator для проверки флага
   - SparkSubmitOperator для записи
3. Добавить переменные и connection для новой БД
4. Добавить зависимость в DAG граф

## Контакты

При возникновении проблем:
- Проверьте логи задач в Airflow UI
- Проверьте Airflow Variables и Connections
- Проверьте доступность Spark, Hive, S3 и целевых БД

