from pyspark.sql.functions import col, year, quarter, month, hour, dayofmonth, dayofweek, explode, weekofyear, quarter, date_format
from src.load.load_dw import load_to_postgres
from prefect import task

@task
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

