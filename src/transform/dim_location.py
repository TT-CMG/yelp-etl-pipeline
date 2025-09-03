from pyspark.sql.functions import col
from config.logger_config import logger
from prefect import task

@task
def transform_dim_location(spark, location_df):
  try:
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
    logger.info("✅ Successful transform dim_location!")
    return df_location
  except Exception as e:
    logger.error("❌ Failed to transform dim_location: %s", e)
    raise

