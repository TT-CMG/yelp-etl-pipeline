import os
import pandas as pd
from config.settings import BASE_DIR,DATA_RAW_DIR, DATA_SUBSAMPLING_DIR
from src.utils.db import get_connection
import pandas as pd
import json
from pyspark.sql import SparkSession
from src.utils.db import load_db_config
from config.logger_config import logger

def extract_file(spark, data_path):
    try:
        print(">>> Start extract.py")
        complete_data_path = os.path.join(DATA_SUBSAMPLING_DIR, data_path)
        # Đọc JSON bằng Spark
        df = spark.read.option("multiline", "false").json(complete_data_path)
        # df.printSchema()
        print(">>> End extract.py")
        return df
    except Exception as e:
        print("Lỗi ở file extract.py:", e)
        return None


def read_data_from_pg(spark, db_name, table_name, schema):
    try:
        logger.info("▶️ Start reading table %s.%s from database %s", schema, table_name, db_name)
        
        df_config = load_db_config(db_name)
        options = {
            "url": df_config["jdbc_url"],
            "dbtable": f"{schema}.{table_name}",
            "user": df_config["user"],
            "password": df_config["password"],
            "driver": "org.postgresql.Driver"
        }

        df = spark.read.format("jdbc").options(**options).load()
        row_count = df.count()
        logger.info("✅ Successfully read %s.%s (rows: %s)", schema, table_name, row_count)
        return df
    except Exception as e:
        logger.error("❌ Failed to read %s.%s from %s: %s", schema, table_name, db_name, e)
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
  
  
  
  

