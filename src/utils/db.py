import psycopg2 as db
from config.settings import DB_CONFIG_DIR
import yaml

def load_db_config(db=""):
    """Đọc config từ file YAML"""
    with open(DB_CONFIG_DIR, "r") as f:
        config = yaml.safe_load(f)
    return config[db]

def get_connection(db_name: str):
    # Load cấu hình kết nối từ file hoặc dict
    db_config = load_db_config(db_name)

    # Kết nối database
    conn = db.connect(
        host=db_config["host"],
        port=db_config["port"],
        dbname=db_config["database"],
        user=db_config["user"],
        password=db_config["password"]
    )
    return conn
