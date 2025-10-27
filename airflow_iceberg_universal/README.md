# Universal Iceberg DAG

Универсальный DAG для чтения Iceberg таблиц из Hive и записи в целевые БД.

## Файлы в папке

- `iceberg_universal_dag.py` - основной DAG файл
- `iceberg_to_postgresql_job.py` - Spark job для PostgreSQL
- `iceberg_to_greenplum_job.py` - Spark job для Greenplum
- `iceberg_to_impala_job.py` - Spark job для Impala (закомментирован)
- `iceberg_to_oracle_job.py` - Spark job для Oracle (закомментирован)
- `UNIVERSAL_DAG_SETUP.md` - документация по настройке

## Быстрый старт

1. Настроить Airflow Variables (см. UNIVERSAL_DAG_SETUP.md)
2. Загрузить jobs в S3:
   ```bash
   aws s3 cp iceberg_to_postgresql_job.py s3://more/spark/scripts/
   aws s3 cp iceberg_to_greenplum_job.py s3://more/spark/scripts/
   ```
3. Разместить DAG в Airflow:
   ```bash
   cp iceberg_universal_dag.py /path/to/airflow/dags/
   ```
4. Запустить через Airflow UI

## Активные задачи

По умолчанию активны:
- PostgreSQL (ENABLE_POSTGRESQL = "true")
- Greenplum (ENABLE_GREENPLUM = "true")

Неактивны (закомментированы):
- Impala (ENABLE_IMPALA = "false")
- Oracle (ENABLE_ORACLE = "false")

Подробности см. в UNIVERSAL_DAG_SETUP.md

