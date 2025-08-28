from config.logger_config import logger
from src.load.load_stg import load_raw_data_to_stg
from scripts.init_schema import init_schema
from src.utils.spark import spark_session
from src.load.load_dw import load_to_postgres, load_to_postgres_via_copy
from src.extract.extract import read_data_from_pg

from src.transform.dim_time import create_dim_time
from src.transform.dim_user import transform_dim_user
from src.transform.dim_business import transform_dim_business, transform_category, transform_bridge_category
from src.transform.fact_review import transform_fact_review
from src.transform.dim_location import transform_dim_location
from src.transform.fact_checkin import transform_fact_checkin

import time

def run_dim_user_pipeline(spark):
  stg_user = read_data_from_pg(spark, 'postgres', 'stg_user', 'stg')
  dim_user = transform_dim_user(spark, stg_user)
  load_to_postgres(dim_user, 'dw.dim_user')

def run_dim_time_pipeline(spark):
  dim_time = create_dim_time(spark, '2005-03-01', '2022-01-20')
  load_to_postgres(dim_time, 'dw.dim_time')
    
def run_dim_business_pipeline(spark):
  stg_business = read_data_from_pg(spark, 'postgres', 'stg_business', 'stg')
  dim_location = read_data_from_pg(spark, 'postgres', 'dim_location', 'dw')
  dim_business = transform_dim_business(spark,stg_business, dim_location)
  load_to_postgres(dim_business, 'dw.dim_business')

def run_dim_category_pipeline(spark):
  stg_business = read_data_from_pg(spark, 'postgres', 'stg_business', 'stg')
  dim_category = transform_category(spark, stg_business)
  
  load_to_postgres(dim_category, 'dw.dim_category')
  
def run_bridge_category_pipeline(spark):
  stg_business = read_data_from_pg(spark, 'postgres', 'stg_business', 'stg')
  dim_business = read_data_from_pg(spark, 'postgres', 'dim_business', 'dw')
  dim_category = read_data_from_pg(spark, 'postgres', 'dim_category', 'dw')
  
  bridge_category = transform_bridge_category(spark, stg_business, dim_business, dim_category)
  
  load_to_postgres(bridge_category, 'dw.bridge_category')

def run_dim_locatin_pipeline(spark):
  stg_business = read_data_from_pg(spark, 'postgres', 'stg_business', 'stg')
  
  dim_location = transform_dim_location(spark, stg_business)
  
  load_to_postgres(dim_location, 'dw.dim_location')
  
def run_fact_review_pipeline(spark):
  stg_review = read_data_from_pg(spark, 'postgres', 'stg_review', 'stg')
  dim_user = read_data_from_pg(spark, 'postgres', 'dim_user', 'dw')
  dim_business = read_data_from_pg(spark, 'postgres', 'dim_business', 'dw')
  dim_time = read_data_from_pg(spark, 'postgres', 'dim_time', 'dw')

  fact_review = transform_fact_review(spark, stg_review, dim_user, dim_business, dim_time)
  
  load_to_postgres(fact_review, 'dw.fact_review')

def run_fact_checkin_pipeline(spark):
  stg_checkin = read_data_from_pg(spark, 'postgres', "stg_checkin", "stg")
  dim_business = read_data_from_pg(spark, 'postgres', "dim_business", "dw")
  dim_time = read_data_from_pg(spark, 'postgres', "dim_time", "dw")
  
  
  fact_checkin = transform_fact_checkin(spark, stg_checkin, dim_business, dim_time)
  start = time.time()
  load_to_postgres_via_copy(fact_checkin, 'dw.fact_checkin')
  end = time.time()
  print(f"loading time: {end - start: .2f}")
  # load_to_postgres(fact_checkin, 'dw.fact_checkin')
  
def etl_pipeline():
  try:
    spark = spark_session()
    
    # # init db
    # init_schema()
    
    # # extract and load to stg
    # load_raw_data_to_stg()
    
    # run_dim_locatin_pipeline(spark=spark)
    # run_dim_time_pipeline(spark=spark)
    # run_dim_business_pipeline(spark=spark)
    # run_dim_category_pipeline(spark=spark)
    # run_bridge_category_pipeline(spark=spark)
    # run_dim_user_pipeline(spark=spark)
    # run_fact_review_pipeline(spark=spark)
    run_fact_checkin_pipeline(spark=spark)
    # extract and load to dw
    # transform and load data to dw

    spark.stop()
  except Exception as e:
    logger.error("Lỗi ở ETL, %s", e)
    raise
  