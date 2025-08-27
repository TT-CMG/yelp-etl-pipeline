from src.utils.db import get_connection
from pathlib import Path
from config.settings import SQL_DIR
from config.logger_config import logger

def load_stg_to_dw(conn):
  try:
    cur = conn.cursor()
    for folder in ["dim"]:
      folder_path = Path(SQL_DIR) / "dml" / folder
      for sql_file in sorted(folder_path.glob("*.sql")):
        try:
          logger.info("🔹 Executing %s", sql_file)
          with open(sql_file, "r") as f:
              sql = f.read()
              cur.execute(sql)
          conn.commit()
          logger.info("✅ Successful load data to %s", sql_file)
        except Exception as e:
          conn.rollback()
          logger.error("❌ Failed to load data from %s", sql_file)
  except Exception as e:
    logger.error("❌ Failed to load data from %s", sql_file)
  finally:
    conn.close()