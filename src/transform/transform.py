import os
from functools import reduce
from pyspark.sql.functions import col, to_date, to_timestamp, year, quarter, month, hour, dayofmonth, dayofweek, to_timestamp, explode, split, trim, lower, min as spark_min, max as spark_max, weekofyear, quarter, date_format, length, expr, count
from pyspark.sql import SparkSession
from config.settings import DATA_RAW_DIR, DATA_SUBSAMPLING_DIR
from src.extract.extract import extract_file, read_data_from_pg
from src.load.load_dw import load_to_postgres
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import IntegerType, DoubleType

def transform_dim_user(spark, df_user):
  try:
      print(">>> Start transform_dim_user")
      df_user = df_user.withColumn(
            "yelping_since",
            to_timestamp("yelping_since", "yyyy-MM-dd HH:mm:ss")
        )
      compliment_cols = [
        "compliment_hot","compliment_more","compliment_profile","compliment_cute",
        "compliment_list","compliment_note","compliment_plain","compliment_cool",
        "compliment_funny","compliment_writer","compliment_photos"
      ]
      # Chuẩn hóa schema và ép kiểu cột
      dim_user_df = df_user.select(
          col("user_id"),
          col("name"),
          # ép kiểu date từ chuỗi (giữ yyyy-MM-dd)
          #to_date(col("yelping_since"), "yyyy-MM-dd").alias("yelping_since"),
          col("yelping_since"),
          col("review_count").cast(IntegerType()),
          col("useful").cast(IntegerType()),
          col("funny").cast(IntegerType()),
          col("cool").cast(IntegerType()),
          col("fans").cast(IntegerType()),
          col("average_stars").cast(DoubleType()),
          reduce(lambda a, b: a + b, [col(c) for c in compliment_cols]).alias("total_compliments")
      )

      print(">>> End transform_dim_user")
      return dim_user_df

  except Exception as e:
      print(f"❌ Error in transform_user: {e}")
      return None
  
def transform_category(spark, stg_business):
  df_exploded = (
    stg_business.withColumn("category", explode(split(col("categories"), ",\\s*")))
      .withColumn("category", trim(col("category")))
      .filter(col("category").isNotNull() & (col("category") != ""))
      .withColumn("category", lower(col("category")))
  )
  # 1. dim_category
  df_category = ( 
    df_exploded
      .select("category").distinct()
      .withColumnRenamed("category", "category_name")
  )
  
  return df_category

def transform_bridge_category(spark, stg_business, dim_business, dim_category):
  stg_business.show(5, truncate = False)
  dim_business.show(5, truncate = False)
  dim_category.show(5, truncate = False)
  
  df_exploded = (
    stg_business.withColumn("category", explode(split(col("categories"), ",\\s*")))
      .withColumn("category", trim(col("category")))
      .filter(col("category").isNotNull() & (col("category") != ""))
      .withColumn("category", lower(col("category")))
      .select("category", "business_id")
      .withColumnRenamed("category", "category_name")
  )
  
  df_exploded.show(5, truncate = False)
  # 2. dim_bridge_category
  df_bridge = (
    df_exploded
    .join(dim_business.select("business_id", "business_key"), on="business_id", how="inner")
    .join(dim_category.select("category_name", "category_key"), on="category_name", how="inner")
    .select("business_key", "category_key")
    .dropDuplicates()
)
  
  df_bridge.show(5, truncate = False)
  
  return df_bridge

def transform_dim_business(spark, business_df, dim_location_df):
  try:
    print(">>> Start transform_dim_business")
    dim_business_df = business_df \
      .join(dim_location_df,
            (business_df["address"] == dim_location_df["address"]) &
            (business_df["city"] == dim_location_df["city"]) &
            (business_df["state"] == dim_location_df["state"]) &
            (business_df["postal_code"] == dim_location_df["postal_code"]) &
            (business_df["latitude"] == dim_location_df["latitude"]) &
            (business_df["longitude"] == dim_location_df["longitude"]),
            how="left") \
      .select(
          col("business_id"),
          col("name"),
          col("stars"),
          col("review_count"),
          (col("is_open") == 1).alias("is_open"), 
          col("location_key")
      ).dropDuplicates()
    print(">>> End transform_dim_business")
    return dim_business_df
  except Exception as e:
    print("❌ Error in transform_business:", e)
    return None

def transform_dim_location(spark, location_df):
  try:
    print(">>> Start transform_dim_business")
    df_location = (
        location_df
        .select(
            col("address"),
            col("city"),
            col("state"),
            col("postal_code"),
            col("latitude").cast("decimal(10,6)"),
            col("longitude").cast("decimal(10,6)")
        )
        .dropDuplicates()
    )
    print(">>> End transform_dim_business")
    return df_location
  except Exception as e:
    print("❌ Error in transform_location:", e)
    return None

def create_dim_time(spark, start_date: str, end_date: str):
    df = spark.sql(f"""
        SELECT sequence(
            to_timestamp('{start_date}'),
            to_timestamp('{end_date}'),
            interval 1 hour
        ) as date_seq
    """)

    df = df.select(explode(col("date_seq")).alias("full_date"))  # ✅ đổi thành full_date

    dim_time = (
    df.withColumn("time_key", date_format(col("full_date"), "yyyyMMddHH").cast("int"))
      .withColumn("year", year("full_date"))
      .withColumn("quarter", quarter("full_date"))
      .withColumn("month", month("full_date"))
      .withColumn("day", dayofmonth("full_date"))
      .withColumn("hour", hour("full_date"))
      .withColumn("day_of_week", dayofweek("full_date"))
      .withColumn("week_of_year", weekofyear("full_date"))
      .withColumn("day_name", date_format(col("full_date"), "EEEE"))
      .withColumn("month_name", date_format(col("full_date"), "MMMM"))
      .withColumn("is_weekend", col("day_of_week").isin([1, 7]))  # Sunday=1, Saturday=7
      .orderBy("full_date")
    )

    return dim_time

def get_date_range(df, colname="date"):
    df = df.withColumn("date", to_date(col(colname)))
    result = df.select(spark_min("date").alias("min_date"),
                       spark_max("date").alias("max_date")).collect()[0]
    return result["min_date"], result["max_date"]

def transform_review(spark, stg_review, dim_user, dim_business, dim_time):
  # chuyển df_review thành int
  df_review = (
    stg_review
    .withColumn("text_length", length(col("text")))
    .withColumn("review_date", to_date(col("review_date")))
  )
  # join để lấy user_key
  df_review = (
      df_review.join(dim_user.select("user_id", "user_key"), on = "user_id", how = "left")
    )
  
  # join để lấy business_key
  df_review = (
    df_review
      .join(dim_business.select("business_key", "business_id"), on = "business_id",
      how = "left")
  )
  
  # join để lấy time_key
  df_review = (
    df_review
      .join(dim_time.select("time_key", "full_date"),
      df_review["review_date"] == dim_time["full_date"],
      how = "left")
  )
  
  df_review = df_review.select(
    "review_id",
    "user_key",
    "business_key",
    "time_key",
    "stars",
    "useful",
    "funny",
    "cool",
    "text_length"
  )
  return df_review

def transform_checkin(spark, stg_checkin, dim_business, dim_time):
  df_checkin = (
    stg_checkin
    .withColumn("checkin_time", explode(split(col("checkin_date"), ",")))
    .withColumn("checkin_time", to_timestamp(trim("checkin_time")))
  )
  
  df_checkin = (
    df_checkin.join(dim_business.select("business_key", "business_id"), on = "business_id", how = "left")
  )
  df_checkin = df_checkin.withColumn(
    "time_key",
    expr("year(checkin_time)*1000000 + month(checkin_time)*10000 + day(checkin_time)*100 + hour(checkin_time)")
  )
  df_checkin = (
    df_checkin
      .groupby("business_key", "time_key")
      .agg(count("*").alias("checkin_count"))
  )
  
  df_checkin = (
    df_checkin
      .join(dim_time.select("time_key"), 
      on = "time_key",
      how = "left")
  )
  
  df_checkin = df_checkin.select(
    "business_key",
    "time_key",
    "checkin_count"
  )
  
  
  
  return df_checkin

if __name__ == '__main__':
  
  spark = SparkSession.builder \
        .appName("etl_pipeline") \
        .config("spark.driver.memory", "4g")\
        .config("spark.executor.memory", "4g")\
        .config("spark.sql.shuffle.partitions", "8")\
        .config("spark.jars", "/home/root/jars/postgresql-42.7.4.jar")\
        .getOrCreate()
  
  stg_checkin = read_data_from_pg(spark, 'postgres', 'stg_checkin', 'stg')
  dim_business = read_data_from_pg(spark, 'postgres', 'dim_business', 'dw')
  dim_time = read_data_from_pg(spark, 'postgres', 'dim_time', 'dw')
  fact_checkin = transform_checkin(spark, stg_checkin, dim_business, dim_time)
  
  load_to_postgres(fact_checkin, 'dw.fact_checkin')
  
  
  # stg_review = read_data_from_pg(spark, 'postgres', 'stg_review', 'stg')
  # dim_user = read_data_from_pg(spark, 'postgres', 'dim_user', 'dw')
  # dim_business = read_data_from_pg(spark, 'postgres', 'dim_business', 'dw')
  # dim_time = read_data_from_pg(spark, 'postgres', 'dim_time', 'dw')
  
  # fact_review = transform_review(spark, stg_review, dim_user, dim_business, dim_time)
  # fact_review.show(5, truncate = False)
  # load_to_postgres(fact_review, 'dw.fact_review')
  
  spark.stop()