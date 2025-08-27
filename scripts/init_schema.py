from src.utils.db import get_connection
from pathlib import Path
from config.settings import SQL_DIR
from config.logger_config import logger

def init_schema():
  conn = None
  try:
    conn = get_connection("postgres")
    with conn.cursor() as cur:
      # Run schema.sql
      schema_file = Path(SQL_DIR) / "ddl" / "schema" / "schema.sql"
      with open(schema_file, "r") as f:
        logger.info("🔹 Executing %s", schema_file)
        cur.execute(f.read())
      conn.commit()

      # Run all other ddl files
      for folder in ["stg", "dw/dim", "dw/fact"]:
        folder_path = Path(SQL_DIR) / "ddl" / folder
        for sql_file in sorted(folder_path.glob("*.sql")):
          try:
            logger.info("🔹 Executing %s", sql_file)
            with open(sql_file, "r") as f:
                cur.execute(f.read())
            conn.commit()
            logger.info("✅ Executed %s", sql_file)
          except Exception:
            conn.rollback()
            logger.exception("❌ Failed to execute %s", sql_file)
            raise

    logger.info("✅ All tables created successfully!")

  except Exception:
    if conn:
        conn.rollback()
    logger.exception("❌ Failed to initialize schema")
    raise  
  
  finally:
    if conn:
        conn.close()
        logger.info("🔒 Connection closed.")
