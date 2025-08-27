from pyspark.sql.functions import col, explode, split, trim, lower
from config.logger_config import logger


def transform_category(spark, stg_business):
  try:
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
    
    logger.info("✅ Successful transform dim_category!")
    return df_category
  except Exception as e:
    logger.error("❌ Failed to transform dim_category: %s", e)
    raise

def transform_bridge_category(spark, stg_business, dim_business, dim_category):
  try:
    df_exploded = (
      stg_business.withColumn("category", explode(split(col("categories"), ",\\s*")))
        .withColumn("category", trim(col("category")))
        .filter(col("category").isNotNull() & (col("category") != ""))
        .withColumn("category", lower(col("category")))
        .select("category", "business_id")
        .withColumnRenamed("category", "category_name")
    )
    
    # 2. dim_bridge_category
    df_bridge = (
      df_exploded
      .join(dim_business.select("business_id", "business_key"), on="business_id", how="inner")
      .join(dim_category.select("category_name", "category_key"), on="category_name", how="inner")
      .select("business_key", "category_key")
      .dropDuplicates()
    )
    logger.info("✅ Successful transform bridge_category!")
    return df_bridge
  except Exception as e:
    logger.error("❌ Failed to transform bridge_category: %s", e)
    raise
  
  
def transform_dim_business(spark, business_df, dim_location_df):
  try:
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
    
    logger.info("✅ Successful transform dim_business!")
    return dim_business_df
  except Exception as e:
    logger.error("❌ Failed to transform dim_business: %s", e)
    raise