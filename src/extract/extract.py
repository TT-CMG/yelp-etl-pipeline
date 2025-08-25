import os
import pandas as pd
from config.settings import BASE_DIR,DATA_RAW_DIR, DATA_SUBSAMPLING_DIR
from src.utils.db import get_connection
import pandas as pd
import json
from pyspark.sql import SparkSession
from src.utils.db import load_db_config

def extract_file(spark, data_path):
    try:
        print(">>> Start extract.py")
        complete_data_path = os.path.join(DATA_SUBSAMPLING_DIR, data_path)
        # Đọc JSON bằng Spark
        df = spark.read.option("multiline", "false").json(complete_data_path)
        print("Tên cột:", df.columns)
        print("Schema:")
        df.printSchema()
        print(">>> End extract.py")
        return df
    except Exception as e:
        print("Lỗi ở file extract.py:", e)
        return None


def read_data_from_pg(spark, db_name, table_name, schema):
    """
    Đọc data từ Postgres vào Spark DataFrame
    - db_name: tên db config trong file yaml
    - table_name: tên table
    - schema: tên schema (default = stg)
    """
    try:
        print(f">>> Start read {schema}.{table_name}")
        df_config = load_db_config(db_name)

        options = {
            "url": df_config["jdbc_url"],
            "dbtable": f"{schema}.{table_name}",
            "user": df_config["user"],
            "password": df_config["password"],
            "driver": "org.postgresql.Driver"
        }

        df = spark.read.format("jdbc").options(**options).load()
        print(f"✅ Load xong {schema}.{table_name}, số dòng: {df.count()}")
        return df
    except Exception as e:
        print("❌ Lỗi ở file extract/read_data_from_pg", e)
        return None

    
if __name__ == '__main__':
    print("Let's go")
    spark = SparkSession.builder \
        .appName("etl_pipeline") \
        .config("spark.driver.memory", "4g")\
        .config("spark.executor.memory", "4g")\
        .config("spark.sql.shuffle.partitions", "8")\
        .config("spark.jars", "/home/root/jars/postgresql-42.7.4.jar")\
        .getOrCreate()
    print("Let's go")
    df = read_data_from_pg(spark, 'postgres', 'stg_business')
    print("Let's go")
    df.show(5, truncate=False)
  
  
  
  

