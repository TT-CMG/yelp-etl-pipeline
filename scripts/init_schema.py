from src.utils.db import get_connection
from pathlib import Path
from config.settings import SQL_DIR

def create_table():
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
    cur.close()
    conn.close()
    print("✅ All tables created successfully!")
  except Exception as e:
    print("Lỗi ở file scripts/init_schema.py", e)

if __name__ == '__main__':
  create_table()