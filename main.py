from pyspark.sql import SparkSession
from config.logger_config import logger
from src.load.load_stg import load_raw_data_to_stg
from scripts.init_schema import init_schema
from src.pipeline.run_pipeline import etl_pipeline
  

  
if __name__ == '__main__':
  etl_pipeline()
  
  
  
  
  
  