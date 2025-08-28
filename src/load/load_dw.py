from src.utils.db import load_db_config
from config.logger_config import logger
from config.settings import DATA_EXPORT_DIR
import tempfile
import psycopg2 as db
from config.logger_config import logger
from src.utils.db import load_db_config
import os
import shutil
from src.utils.db import get_connection


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
    

def load_to_postgres_via_copy(df, table_name: str):
    tmpdir = tempfile.mkdtemp()
    DATA_EXPORT = os.path.join(DATA_EXPORT_DIR, "export.csv")
    try:
        # 1. Xuất DataFrame ra file CSV tạm
        (
            df.coalesce(1)
              .write
              .option("header", "true")
              .mode("overwrite")
              .csv(tmpdir)
        )

        # 2. Spark sẽ tạo file dạng part-0000.csv => move về export.csv
        export_dir = os.path.dirname(DATA_EXPORT)
        os.makedirs(export_dir, exist_ok=True)  # đảm bảo thư mục tồn tại

        for f in os.listdir(tmpdir):
            if f.startswith("part-") and f.endswith(".csv"):
                src = os.path.join(tmpdir, f)
                shutil.move(src, DATA_EXPORT)   # thay vì os.rename

        # 3. Kết nối PostgreSQL
        conn = get_connection('postgres')
        cur = conn.cursor()
        columns = df.columns
        col_str = ",".join(columns)
        # 4. COPY dữ liệu vào Postgres
        with open(DATA_EXPORT, "r", encoding="utf-8") as f:
            sql = f"""
                COPY {table_name} ({col_str})
                FROM STDIN WITH CSV HEADER
                """
            cur.copy_expert(sql, f)

        conn.commit()
        cur.close()
        conn.close()

        logger.info("✅ Successfully loaded to %s via COPY", table_name)

    except Exception as e:
        logger.error("❌ Failed to load %s via COPY: %s", table_name, e)
        raise
    finally:
        # 5. Xóa file tạm + thư mục tạm
        try:
            if os.path.exists(DATA_EXPORT):
                os.remove(DATA_EXPORT)
            if os.path.exists(tmpdir):
                shutil.rmtree(tmpdir)   # xoá cả thư mục và file con
        except Exception as cleanup_error:
            logger.warning("⚠️ Could not clean up temp files: %s", cleanup_error)