
from pyspark.sql.functions import to_timestamp, col
from pyspark.sql.types import IntegerType, DoubleType
from functools import reduce
from config.logger_config import logger

def transform_dim_user(spark, df_user):
  try:
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

    logger.info("✅ Successful transform dim_user!")
    return dim_user_df
  except Exception as e:
      logger.error("❌ Failed to transform dim_user: %s", e)
      raise