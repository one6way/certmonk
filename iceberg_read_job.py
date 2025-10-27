#!/usr/bin/env python3
"""
Spark Job для чтения Iceberg таблиц через Hive
Запускается через Airflow SparkSubmitOperator
"""

import os
import sys
from pyspark.sql import SparkSession

def main():
    """Основная функция"""
    
    # Получаем переменные из окружения
    bucket = os.getenv('ICEBERG_BUCKET', 'core')
    schema = os.getenv('ICEBERG_SCHEMA', 'core_stage')
    table = os.getenv('ICEBERG_TABLE', 'source_system')
    hive_uri = os.getenv('HIVE_METASTORE_URI')
    s3_endpoint = os.getenv('S3_ENDPOINT', 'http://minio.minio:9000')
    
    spark_master = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')
    
    print(f"="*60)
    print(f"Чтение Iceberg таблицы через Hive")
    print(f"="*60)
    print(f"Bucket: {bucket}")
    print(f"Схема: {schema}")
    print(f"Таблица: {table}")
    print(f"S3 Endpoint: {s3_endpoint}")
    print(f"Hive: {hive_uri}")
    print(f"Spark Master: {spark_master}")
    print(f"="*60)
    
    # Создаем Spark
    print("\nСоздание Spark сессии...")
    
    spark = (SparkSession.builder
        .appName(f"iceberg_{schema}_{table}")
        .master(spark_master)
        
        # ICEBERG поддержка
        .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        
        # HIVE поддержка
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.defaultDatabase", schema)
        .config("hive.exec.default.database", schema)
        .config("hive.metastore.uris", hive_uri)
        .config("hive.metastore.warehouse.dir", f"s3a://{bucket}/warehouse")
        .config("hive.metastore.client.connect.retry.delay", "5")
        .config("hive.metastore.client.socket.timeout", "1800")
        .config("hive.metastore.connect.retries", "3")
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        
        # S3 настройки
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID'))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY'))
        
        # Оптимизации
        .config("spark.eventLog.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        .getOrCreate())
    
    print("Spark сессия создана!")
    
    try:
        # Принудительно переключаемся на нужную базу
        spark.sql(f"USE {schema}")
        print(f"Переключились на базу: {schema}")
        
        # Читаем таблицу
        sql_query = f"SELECT * FROM {schema}.{table}"
        print(f"\nSQL запрос: {sql_query}")
        
        df = spark.sql(sql_query)
        
        print("\nУСПЕХ! Таблица прочитана!")
        print("Схема таблицы:")
        df.printSchema()
        
        print("\nПервые 10 строк:")
        df.limit(10).show(truncate=False)
        
        # Подсчет записей
        print("\nПодсчет записей...")
        count = df.count()
        print(f"Всего записей: {count:,}")
        
        # Опционально: сохраняем результаты
        # output_path = f"s3a://{bucket}/output/{schema}/{table}/"
        # df.write.mode("overwrite").parquet(output_path)
        # print(f"\nРезультаты сохранены в: {output_path}")
        
    except Exception as e:
        print(f"\nОШИБКА при чтении таблицы: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()
        print("\nЗавершено")

if __name__ == "__main__":
    main()

