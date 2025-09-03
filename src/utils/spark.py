from pyspark.sql import SparkSession
from config.logger_config import logger
import os
# .config("spark.jars", "/home/root/jars/postgresql-42.7.4.jar")\

def spark_session():
  try:
    print(os.path.exists("D:/Bigdata/spark-3.5.6/jars/postgresql-42.7.4.jar"))
    spark = SparkSession.builder \
        .appName("etl_pipeline") \
        .config("spark.driver.memory", "4g")\
        .config("spark.executor.memory", "4g")\
        .config("spark.sql.shuffle.partitions", "8")\
        .getOrCreate()
    return spark
  except Exception as e:
    logger.error("❌ Lỗi kết nối với Postgres: %s", e)
    return None  