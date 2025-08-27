from src.utils.db import get_connection
from pathlib import Path
from config.settings import SQL_DIR

def load_stg_to_dw(conn):
  try:
    cur = conn.cursor()
    
    for folder in ["dim"]:
      folder_path = Path(SQL_DIR) / "dml" / folder
      for sql_file in sorted(folder_path.glob("*.sql")):
        print(f"🔹 Executing {sql_file}")
        with open(sql_file, "r") as f:
            sql = f.read()
            cur.execute(sql)
        conn.commit()
        print(f"✅ Data was loaded to {sql_file} successfully!")
  except Exception as e:
    conn.rollback()
    cur.close()
    conn.close()
    print("Lỗi ở file scripts/load_stg_to_dw.py", e)

if __name__ == '__main__':
  conn = get_connection('postgres')
  load_stg_to_dw(conn)
  conn.close()