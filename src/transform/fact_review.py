from pyspark.sql.functions import to_date, col, length
from config.logger_config import logger


def transform_fact_review(spark, stg_review, dim_user, dim_business, dim_time):
  try:
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
    logger.info("✅ Successful transform fact_review!")
    return df_review
  except Exception as e:
    logger.error("❌ Failed to transform fact_review: %s", e)
    raise