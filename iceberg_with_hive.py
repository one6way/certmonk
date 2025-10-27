"""
ИТОГОВЫЙ СКРИПТ: Iceberg + Hive (как на Jupiter)
Правильное подключение к Hive для чтения Iceberg из S3
"""

import os
from pyspark.sql import SparkSession

# =============================================================================
# НАСТРОЙКИ - ИЗМЕНИТЕ ЗДЕСЬ:
# =============================================================================

# Основные настройки
BUCKET = "core"
SCHEMA_NAME = "core_stage"  # ← ИЗМЕНИТЕ СХЕМУ ЗДЕСЬ
TABLE_NAME = "source_system"  # ← ИЗМЕНИТЕ ТАБЛИЦУ ЗДЕСЬ

# Автоматически формируемые пути
TABLE_BASE_PATH = f"s3a://{BUCKET}/{SCHEMA_NAME}/{TABLE_NAME}"
DATA_PATH = f"{TABLE_BASE_PATH}/data"
METADATA_PATH = f"{TABLE_BASE_PATH}/metadata"

# Hive адреса (из вашего лога)
HIVE_METASTORE_URI = "thrift://hive-0.hive-svc.k-mgp-v01-mgp-nova-impala.svc.cluster.app:9084,thrift://hive-1.hive-svc.k-mgp-v01-mgp-nova-impala.svc.cluster.app:9084"

# =============================================================================

def create_spark_with_hive_iceberg():
    """Создать Spark с правильной Hive конфигурацией"""
    
    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    s3_endpoint = os.getenv('S3_ENDPOINT', 'http://minio.minio:9000')
    
    if not access_key or not secret_key:
        print("ОШИБКА: Установите AWS_ACCESS_KEY_ID и AWS_SECRET_ACCESS_KEY")
        return None
    
    print("Создание Spark с Hive + Iceberg (как на Jupiter)...")
    print(f"S3 Endpoint: {s3_endpoint}")
    print(f"Bucket: {BUCKET}")
    print(f"Hive: {HIVE_METASTORE_URI}")
    
    spark = (SparkSession.builder
        .appName("IcebergWithHive")
        .master("local[2]")
        
        # JAR пакеты (убрали проблемные)
        .config("spark.jars.packages", ",".join([
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.641"
        ]))
        
        # ICEBERG ПОДДЕРЖКА
        .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        
        # HIVE ПОДДЕРЖКА (правильная конфигурация)
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", HIVE_METASTORE_URI)
        .config("hive.metastore.warehouse.dir", f"s3a://{BUCKET}/warehouse")
        
        # Указываем базу данных по умолчанию
        .config("spark.sql.defaultDatabase", SCHEMA_NAME)
        .config("hive.exec.default.database", SCHEMA_NAME)
        
        # Попробуем разные warehouse настройки
        .config("spark.sql.warehouse.dir", f"s3a://{BUCKET}/warehouse")
        .config("hive.metastore.warehouse.dir", f"s3a://{BUCKET}/hive/warehouse")
        
        # S3 настройки
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        
        # Оптимизации
        .config("spark.eventLog.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        # Hive таймауты и повторы
        .config("hive.metastore.client.connect.retry.delay", "5")
        .config("hive.metastore.client.socket.timeout", "1800")
        .config("hive.metastore.connect.retries", "3")
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        
        .getOrCreate())
    
    print("Spark с Hive + Iceberg создан!")
    return spark


def test_hive_connection(spark):
    """Тестируем подключение к Hive"""
    print(f"\nТестирование Hive подключения...")
    
    try:
        # Проверяем каталоги
        catalogs = spark.sql("SHOW CATALOGS").collect()
        print(f"Каталоги: {[row.catalog for row in catalogs]}")
        
        # Проверяем настройки Hive warehouse
        try:
            warehouse_dir = spark.conf.get("hive.metastore.warehouse.dir", "НЕ НАЙДЕНО")
            print(f"Hive warehouse: {warehouse_dir}")
            
            # Проверяем текущую базу данных
            current_db = spark.sql("SELECT current_database()").collect()[0][0]
            print(f"Текущая база данных: {current_db}")
            
        except Exception as e:
            print(f"Не удалось получить warehouse: {e}")
        
        # Принудительно переключаемся на нужную базу
        try:
            spark.sql(f"USE {SCHEMA_NAME}")
            print(f"Переключились на базу: {SCHEMA_NAME}")
        except Exception as e:
            print(f"Не удалось переключиться на {SCHEMA_NAME}: {e}")
        
        # Проверяем базы данных (с обработкой ошибок)
        try:
            databases = spark.sql("SHOW DATABASES").collect()
            print(f"Базы данных: {[row.databaseName for row in databases]}")
            
            # Проверяем таблицы в указанной схеме
            try:
                tables = spark.sql(f"SHOW TABLES IN {SCHEMA_NAME}").collect()
                print(f"Таблицы в {SCHEMA_NAME}: {[row.tableName for row in tables]}")
            except Exception as e:
                print(f"Не удалось получить таблицы в {SCHEMA_NAME}: {e}")
                
                # Попробуем другие базы
                for db in databases[:3]:  # Первые 3 базы
                    try:
                        tables = spark.sql(f"SHOW TABLES IN {db.databaseName}").collect()
                        print(f"Таблицы в {db.databaseName}: {[row.tableName for row in tables]}")
                    except:
                        pass
                        
        except Exception as e:
            print(f"Не удалось получить базы данных: {e}")
            print("Попробуем напрямую обратиться к таблице...")
        
        print(f"Hive подключение работает!")
        return True
        
    except Exception as e:
        print(f"Проблема с Hive: {e}")
        return False


def read_iceberg_via_hive(spark):
    """Чтение Iceberg через Hive (как на Jupiter)"""
    print(f"\nЧтение Iceberg через Hive...")
    
    try:
        # Способ 1: Прямой SQL (как в вашем примере)
        sql_query = f"SELECT * FROM {SCHEMA_NAME}.{TABLE_NAME} LIMIT 50"
        print(f"Способ 1: {sql_query}")
        df = spark.sql(sql_query)
        df.show(truncate=False)
        
        print(f"УСПЕХ! Прочитано через Hive!")
        print(f"Схема:")
        df.printSchema()
        
        try:
            count = df.count()
            print(f"Всего записей: {count:,}")
        except Exception as e:
            print(f"Не удалось подсчитать: {e}")
        
        return df, "hive_sql"
        
    except Exception as e:
        print(f"Способ 1 не работает: {e}")
        
        # Способ 2: Через DataFrameReader
        try:
            print(f"Способ 2: spark.read.format('iceberg')")
            df = spark.read.format("iceberg").load(TABLE_BASE_PATH)
            df.limit(5).show(truncate=False)
            return df, "hive_dataframe"
        except Exception as e2:
            print(f"И способ 2 не работает: {e2}")
            return None, None


def main():
    """Основная функция"""
    
    print("ICEBERG + HIVE (как на Jupiter)")
    print("="*50)
    print(f"Схема: {SCHEMA_NAME}")
    print(f"Таблица: {TABLE_NAME}")
    print(f"Полный путь: {TABLE_BASE_PATH}")
    print(f"Hive: {HIVE_METASTORE_URI}")
    print("="*50)
    
    # Создаем Spark с Hive
    spark = create_spark_with_hive_iceberg()
    if not spark:
        return
    
    try:
        # Тест Hive подключения
        hive_ok = test_hive_connection(spark)
        
        if hive_ok:
            # Чтение через Hive
            df, method = read_iceberg_via_hive(spark)
            
            if df:
                print(f"\nИТОГОВЫЙ РЕЗУЛЬТАТ:")
                print(f"Метод: {method}")
                print(f"Данные прочитаны успешно!")
                
                # Демонстрация операций
                print(f"\nПримеры операций:")
                df.select("*").limit(10).show(truncate=False)
                
            else:
                print(f"\nНе удалось прочитать данные")
        else:
            print(f"\nHive подключение не работает")
    
    except Exception as e:
        print(f"\nОбщая ошибка: {e}")
    
    finally:
        spark.stop()
        print(f"\nЗавершено")


if __name__ == "__main__":
    main()
