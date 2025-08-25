from pyspark.sql import SparkSession

def main():
  try:
    spark = SparkSession.builder \
        .appName("etl_pipeline") \
        .config("spark.driver.memory", "4g")\
        .config("spark.executor.memory", "4g")\
        .config("spark.sql.shuffle.partitions", "8")\
        .config("spark.jars", "/home/root/jars/postgresql-42.7.4.jar")\
        .getOrCreate()
    df_test = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://172.30.240.1:5432/yelp_db") \
        .option("dbtable", "(SELECT 1 AS test_col) AS t") \
        .option("user", "postgres") \
        .option("password", "tucmgdtntg") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    df_test.show()
    print("✅ Spark đã kết nối được với Postgres!")
  except Exception as e:
      print("❌ Lỗi kết nối với Postgres:", e)
  
if __name__ == '__main__':
  main()
  # dim_time
  # dim_time = create_dim_time(spark, '2005-03-01', '2022-01-19')
  # load_to_postgres(dim_time, 'dw.dim_time')
  
  # # dim_user
  # stg_user = read_data_from_pg(spark, 'postgres', 'stg_user', 'stg')
  # dim_user = transform_dim_user(spark, stg_user)
  # load_to_postgres(dim_user, 'dw.dim_user')
  
  # # dim_location
  # stg_business = read_data_from_pg(spark, 'postgres', 'stg_business', 'stg' )
  # dim_location = transform_dim_location(spark, stg_business)
  # load_to_postgres(dim_location, 'dw.dim_location')

  # # dim_business
  # r_dim_location = read_data_from_pg(spark, 'postgres', 'dim_location', 'dw')
  # dim_business = transform_dim_business(spark, stg_business, r_dim_location)
  # load_to_postgres(dim_business, 'dw.dim_business')
  
  # # dim_category
  # r_dim_business = read_data_from_pg(spark, 'postgres', 'dim_business', 'dw')
  # dim_category = transform_category(spark, stg_business)
  # load_to_postgres(dim_category, 'dw.dim_category')

  # # bridge_business_category
  # dim_category = read_data_from_pg(spark, 'postgres', 'dim_category', 'dw')
  # bridge_business_category = transform_bridge_category(spark, stg_business, r_dim_business, dim_category)
  # bridge_business_category.show(5, truncate = False)
  # load_to_postgres(bridge_business_category, 'dw.bridge_business_category')