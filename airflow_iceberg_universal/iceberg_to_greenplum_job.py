#!/usr/bin/env python3
"""
Spark Job для чтения Iceberg таблицы из Hive и записи в Greenplum

Airflow Variables:
- ICEBERG_BUCKET, ICEBERG_SCHEMA, ICEBERG_TABLE
- hive_metastore_uri, s3_endpoint
- aws_access_key_id, aws_secret_access_key
- GP_CONN_ID (connection ID в Airflow)
- TARGET_GP_SCHEMA, TARGET_GP_TABLE
- WRITE_MODE (append/overwrite)
"""

import os
import sys
from pyspark.sql import SparkSession


def get_greenplum_connection():
    """Получить параметры подключения к Greenplum"""
    gp_conn_id = os.getenv('GP_CONN_ID', 'greenplum_default')
    
    host = os.getenv('GP_HOST')
    port = os.getenv('GP_PORT', '5432')
    database = os.getenv('GP_DATABASE')
    user = os.getenv('GP_USER')
    password = os.getenv('GP_PASSWORD')
    
    if not all([host, database, user, password]):
        raise ValueError("Missing Greenplum connection parameters")
    
    url = f"jdbc:postgresql://{host}:{port}/{database}"
    
    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }
    
    return url, properties


def read_iceberg_table(spark, bucket, schema, table):
    """Читает Iceberg таблицу из Hive"""
    print(f"Reading Iceberg table: {schema}.{table}")
    
    spark.sql(f"USE {schema}")
    sql_query = f"SELECT * FROM {schema}.{table}"
    
    df = spark.sql(sql_query)
    
    print(f"Schema read successfully")
    df.printSchema()
    
    count = df.count()
    print(f"Total records: {count:,}")
    
    return df


def write_to_greenplum(df, url, properties, schema, table):
    """Записывает DataFrame в Greenplum"""
    full_table = f"{schema}.{table}"
    write_mode = os.getenv('WRITE_MODE', 'append')
    
    print(f"\nWriting to Greenplum: {full_table}")
    print(f"Write mode: {write_mode}")
    
    df.write \
        .mode(write_mode) \
        .jdbc(url=url, table=full_table, properties=properties)
    
    print(f"Successfully written to {full_table}")


def main():
    """Основная функция"""
    
    bucket = os.getenv('ICEBERG_BUCKET', 'core')
    schema = os.getenv('ICEBERG_SCHEMA', 'core_stage')
    table = os.getenv('ICEBERG_TABLE', 'source_system')
    hive_uri = os.getenv('HIVE_METASTORE_URI')
    s3_endpoint = os.getenv('S3_ENDPOINT', 'http://minio.minio:9000')
    
    spark_master = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')
    
    target_schema = os.getenv('TARGET_GP_SCHEMA', 'public')
    target_table = os.getenv('TARGET_GP_TABLE', 'iceberg_data')
    
    print(f"{'='*60}")
    print(f"Iceberg to Greenplum Pipeline")
    print(f"{'='*60}")
    print(f"Source: {schema}.{table}")
    print(f"Target: {target_schema}.{target_table}")
    print(f"{'='*60}")
    
    print("\nCreating Spark session...")
    
    spark = (SparkSession.builder
        .appName(f"iceberg_to_greenplum_{schema}_{table}")
        .master(spark_master)
        
        .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.defaultDatabase", schema)
        .config("hive.exec.default.database", schema)
        .config("hive.metastore.uris", hive_uri)
        .config("hive.metastore.warehouse.dir", f"s3a://{bucket}/warehouse")
        
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID'))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY'))
        
        .config("spark.eventLog.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        
        .getOrCreate())
    
    try:
        df = read_iceberg_table(spark, bucket, schema, table)
        
        url, properties = get_greenplum_connection()
        
        write_to_greenplum(df, url, properties, target_schema, target_table)
        
        print("\nPipeline completed successfully")
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()
        print("\nSpark session closed")


if __name__ == "__main__":
    main()

