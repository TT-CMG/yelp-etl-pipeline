from src.utils.db import get_connection
from pathlib import Path
from config.settings import SQL_DIR
from config.logger_config import logger


def init_schema():
  try:
    conn = get_connection('postgres')
    cur = conn.cursor()
    
    schema_file = Path(SQL_DIR) / "ddl" / "schema" / "schema.sql"
    with open(schema_file, "r") as f:
        cur.execute(f.read())
    # cur.execute("SET search_path TO dw;")
    for folder in ["stg", "dw/dim", "dw/fact"]:
      folder_path = Path(SQL_DIR) / "ddl" /folder
      for sql_file in sorted(folder_path.glob("*.sql")):
          print(f"🔹 Executing {sql_file}")
          with open(sql_file, "r") as f:
              sql = f.read()
              cur.execute(sql)
          conn.commit()
    conn.commit()
    logger.info("✅ All tables created successfully!")
  except Exception as e:
    if conn:
        conn.rollback()
    logger.error("❌ Failed to initialize schema: %s", e)
    raise  
  finally:
    if conn:
        conn.close()
        logger.info("🔒 Connection closed.")