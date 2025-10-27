# Краткая сводка по Airflow DAG для Iceberg

## Созданные файлы

1. **`iceberg_airflow_dag.py`** - DAG для Airflow
2. **`iceberg_read_job.py`** - Spark job скрипт (загружается в S3)
3. **`airflow_hive_setup.md`** - Детальная инструкция
4. **`k8s-hive-secret.yaml`** - Kubernetes манифест

## Что сделано

### 1. DAG (iceberg_airflow_dag.py)
- KubernetesPodOperator для монтирования Hive certificate
- SparkSubmitOperator для запуска job из S3
- Job путь: `s3a://more/spark/scripts/iceberg_read_job.py`
- Переменные читаются из Airflow Variables в runtime

### 2. Job скрипт (iceberg_read_job.py)
- Читает переменные из `os.getenv()`
- Конфигурирует Spark для Iceberg + Hive + S3
- Выводит схему таблицы и первые 10 строк

### 3. Упрощения
- Удалены лишние функции
- Переменные читаются в runtime (не при загрузке DAG)
- Используется упрощенная структура

## Что нужно настроить

### 1. Kubernetes Secret для Hive certificate
```bash
kubectl create secret generic hive-jceks-secret \
  --from-file=hive.jceks=${HOME}/hive.jceks \
  -n airflow
```

### 2. Airflow Variables
Через UI: Admin → Variables → Add
- `ICEBERG_BUCKET` = "core"
- `ICEBERG_SCHEMA` = "core_stage"
- `ICEBERG_TABLE` = "source_system"
- `hive_metastore_uri` = "thrift://..."
- `s3_endpoint` = "http://minio.minio:9000"
- `aws_access_key_id` = "..."
- `aws_secret_access_key` = "..."

### 3. Airflow Connections
- Spark connection (spark_default)
- AWS connection (aws_default) - опционально

### 4. Загрузить job в S3
```bash
aws s3 cp iceberg_read_job.py s3://more/spark/scripts/iceberg_read_job.py
```

### 5. Разместить DAG
```bash
cp iceberg_airflow_dag.py /path/to/airflow/dags/
```

## Запуск

1. Откройте Airflow UI
2. Найдите DAG `iceberg_read_via_hive`
3. Включите его (toggle)
4. Нажмите "Trigger DAG"

## Особенности

- KubernetesPodOperator нужен для запуска Spark подов в K8s
- Job хранится в S3
- Base64 кодирование не требуется (secret создается через kubectl)
- Переменные читаются из job скрипта, не из DAG

