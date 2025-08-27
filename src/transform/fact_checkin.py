
from pyspark.sql.functions import explode, split, col, trim, to_timestamp, date_format, count
from config.logger_config import logger

def transform_fact_checkin(spark, stg_checkin, dim_business, dim_time):
  try:
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
      date_format(col("checkin_time"), "yyyyMMddHH").cast("int")
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
    
    logger.info("✅ Successful transform fact_checkin!")
    return df_checkin
  except Exception as e:
    logger.error("❌ Failed to transform fact_checkin: %s", e)
    raise