import os
from functools import reduce
from pyspark.sql.functions import col, to_date, to_timestamp, year, quarter, month, hour, dayofmonth, dayofweek, to_timestamp, explode, split, trim, lower, min as spark_min, max as spark_max, weekofyear, quarter, date_format, length, expr, count
from pyspark.sql import SparkSession
from config.settings import DATA_RAW_DIR, DATA_SUBSAMPLING_DIR
from src.extract.extract import read_data_from_pg
from src.load.load_dw import load_to_postgres
from pyspark.sql.types import IntegerType, DoubleType



def get_date_range(df, colname="date"):
    df = df.withColumn("date", to_date(col(colname)))
    result = df.select(spark_min("date").alias("min_date"),
                       spark_max("date").alias("max_date")).collect()[0]
    return result["min_date"], result["max_date"]





if __name__ == '__main__':
  
  spark = SparkSession.builder \
        .appName("etl_pipeline") \
        .config("spark.driver.memory", "4g")\
        .config("spark.executor.memory", "4g")\
        .config("spark.sql.shuffle.partitions", "8")\
        .config("spark.jars", "/home/root/jars/postgresql-42.7.4.jar")\
        .getOrCreate()
  
  spark.stop()