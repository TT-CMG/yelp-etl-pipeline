from config.settings import JDBC_URL
from src.utils.db import load_db_config
from config.logger_config import logger

def load_to_postgres(df, table_name, mode="append"):
    try:
        config = load_db_config('postgres')
        # Tạo JDBC URL từ config
        jdbc_url = f"jdbc:postgresql://{config['host']}:{config['port']}/{config['database']}"
        df.write.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", config['user']) \
            .option("password", config['password']) \
            .option("driver", "org.postgresql.Driver") \
            .mode(mode) \
            .save()
        logger.info("Successful loaded to %s", table_name)
        
    except Exception as e:
        logger.error("❌ Failed to load %s: %s", table_name, e)
        raise