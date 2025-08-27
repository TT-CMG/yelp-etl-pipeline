from pyspark.sql import SparkSession
from config.logger_config import logger



def spark_session():
  try:
    spark = SparkSession.builder \
        .appName("etl_pipeline") \
        .config("spark.driver.memory", "4g")\
        .config("spark.executor.memory", "4g")\
        .config("spark.sql.shuffle.partitions", "8")\
        .config("spark.jars", "/home/root/jars/postgresql-42.7.4.jar")\
        .getOrCreate()
    return spark
  except Exception as e:
    logger.error("❌ Lỗi kết nối với Postgres: %s", e)
    return None  