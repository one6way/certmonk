# Инструкция по настройке Airflow DAG для чтения Iceberg через Hive

## 1. Создание Kubernetes Secret для Hive certificate

### Вариант A: Создаем Secret через kubectl

```bash
# 1. Сначала создайте .jceks файл (как в вашем примере из Jupyter)
java -cp '/usr/local/spark/jars/**' org.apache.ranger.credentialapi.buildks create $SPARK_USER \
  -value "<ваш-пароль-AD>" \
  -provider jceks://file/${HOME}/hive.jceks

# 2. Создайте Secret из файла
kubectl create secret generic hive-jceks-secret \
  --from-file=hive.jceks=${HOME}/hive.jceks \
  -n airflow

# 3. Проверьте что Secret создан
kubectl get secret hive-jceks-secret -n airflow
```

### Вариант B: Создаем Secret через YAML

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: hive-jceks-secret
  namespace: airflow
type: Opaque
data:
  hive.jceks: <base64-encoded-content-of-jceks-file>
```

Создайте secret:
```bash
kubectl apply -f hive-jceks-secret.yaml
```

## 2. Настройка Airflow Connections

### Spark Connection

```python
from airflow.models import Connection
from airflow.settings import Session

# Создаем Spark connection
conn = Connection(
    conn_id='spark_default',
    conn_type='spark',
    host='spark-master.spark.svc.cluster.local',  # Ваш Spark Master
    port=7077,
    schema='spark'
)
session = Session()
session.add(conn)
session.commit()
```

Или через UI: Admin → Connections → Add → Тип: Spark

### AWS Connection для S3

```python
conn = Connection(
    conn_id='aws_default',
    conn_type='aws',
    login='YOUR_ACCESS_KEY',  # Access Key
    password='YOUR_SECRET_KEY'  # Secret Key
)
session.add(conn)
session.commit()
```

Или через UI: Admin → Connections → Add → Тип: Amazon Web Services

## 3. Настройка Airflow Variables

```python
from airflow.models import Variable

# Переменные для Iceberg
Variable.set("ICEBERG_BUCKET", "core")
Variable.set("ICEBERG_SCHEMA", "core_stage")
Variable.set("ICEBERG_TABLE", "source_system")

# Hive настройки
Variable.set("hive_metastore_uri", 
    "thrift://hive-0.hive-svc.k-mgp-v01-mgp-nova-impala.svc.cluster.app:9084")

# S3 настройки
Variable.set("s3_endpoint", "http://minio.minio:9000")
Variable.set("aws_access_key_id", "YOUR_KEY")
Variable.set("aws_secret_access_key", "YOUR_SECRET")
```

Или через UI: Admin → Variables → Add

## 4. Создание Python скрипта для выполнения

Создайте файл `/path/to/iceberg_read_job.py`:

```python
import os
from pyspark.sql import SparkSession

# Получаем переменные
bucket = os.getenv('ICEBERG_BUCKET', 'core')
schema = os.getenv('ICEBERG_SCHEMA', 'core_stage')
table = os.getenv('ICEBERG_TABLE', 'source_system')
hive_uri = os.getenv('HIVE_METASTORE_URI')
s3_endpoint = os.getenv('S3_ENDPOINT', 'http://minio.minio:9000')

# Создаем Spark
spark = (SparkSession.builder
    .appName(f"iceberg_{schema}_{table}")
    .master(os.getenv('SPARK_MASTER', 'spark://spark-master:7077'))
    
    # ICEBERG
    .config("spark.sql.extensions", 
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    
    # HIVE
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.sql.defaultDatabase", schema)
    .config("hive.metastore.uris", hive_uri)
    .config("hive.metastore.warehouse.dir", f"s3a://{bucket}/warehouse")
    
    # S3
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID'))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY'))
    
    .config("spark.eventLog.enabled", "false")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    
    .getOrCreate())

# Читаем таблицу
sql_query = f"SELECT * FROM {schema}.{table} LIMIT 100"
df = spark.sql(sql_query)

print(f"Схема таблицы {schema}.{table}:")
df.printSchema()
print("Первые 10 строк:")
df.limit(10).show(truncate=False)

spark.stop()
print("Завершено")
```

## 5. Загрузка Job в S3

```bash
# Загрузите job скрипт в S3 bucket 'more'
aws s3 cp iceberg_read_job.py s3://more/spark/scripts/iceberg_read_job.py

# Или через kubectl
kubectl cp iceberg_read_job.py <pod-name>:/tmp/iceberg_read_job.py
# Затем изнутри pod загрузите в S3

# Проверьте что файл загружен
aws s3 ls s3://more/spark/scripts/
```

## 6. Размещение DAG файла

```bash
# Скопируйте DAG в папку dags
cp iceberg_airflow_dag.py /path/to/airflow/dags/

# Проверьте что DAG подхватился
airflow dags list | grep iceberg
```

## 7. Запуск DAG

### Через UI:
1. Откройте Airflow UI
2. Найдите DAG `iceberg_read_via_hive`
3. Включите его (toggle)
4. Нажмите "Trigger DAG"

### Через CLI:
```bash
airflow dags trigger iceberg_read_via_hive
```

## 8. Проверка логов

```bash
# Смотрим логи задачи
airflow tasks logs iceberg_read_via_hive read_iceberg_table {dag_run_id} {task_instance_id}
```

## Возможные проблемы

### 1. Spark connection не работает
```bash
# Проверьте доступность Spark Master
kubectl get svc -n spark | grep spark-master
```

### 2. S3 недоступен
```bash
# Проверьте MinIO
kubectl get svc -n minio | grep minio
```

### 3. Hive certificate не найден
```bash
# Проверьте Secret
kubectl get secret hive-jceks-secret -n airflow
kubectl describe secret hive-jceks-secret -n airflow
```

## Следующие шаги

После успешной настройки можно:
1. Добавить больше таблиц через Variables
2. Настроить расписание
3. Добавить обработку ошибок
4. Отправку уведомлений
5. Сохранение результатов

