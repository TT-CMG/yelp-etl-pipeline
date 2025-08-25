import os, json, psycopg2
import psycopg2.extras
from src.utils.db import get_connection
# ==========================
# Mappings: table → (columns, json_fields, casts)
# ==========================
MAPPINGS = {
    # "stg.stg_business": {
    #     "file": "data/subsampling/business.json",
    #     "columns": [
    #         "business_id","name","address","city","state","postal_code",
    #         "latitude","longitude","stars","review_count","is_open",
    #         "categories","attributes","hours"
    #     ],
    #     "json_fields": [
    #         "business_id","name","address","city","state","postal_code",
    #         "latitude","longitude","stars","review_count","is_open",
    #         "categories","attributes","hours"
    #     ],
    #     "casts": {
    #         "latitude": float,
    #         "longitude": float,
    #         "stars": float,
    #         "review_count": int,
    #         "is_open": int,
    #         "attributes": json.dumps,
    #         "hours": json.dumps
    #     }
    # }, 
    # "stg.stg_review": {
    #     "file": "data/subsampling/review.json",
    #     "columns": [
    #         "review_id","user_id","business_id","stars","review_date",
    #         "text","useful","funny","cool"
    #     ],
    #     "json_fields": [
    #         "review_id","user_id","business_id","stars","date",
    #         "text","useful","funny","cool"
    #     ],
    #     "casts": {
    #         "stars": float,
    #         "date": str,  # giữ string, sang dwh sẽ ép TIMESTAMP
    #         "useful": int,
    #         "funny": int,
    #         "cool": int
    #     }
    # },
    # "stg.stg_user": {
    #     "file": "data/subsampling/user.json",
    #     "columns": [
    #         "user_id","name","review_count","yelping_since","useful","funny",
    #         "cool","fans","average_stars","compliment_hot","compliment_more",
    #         "compliment_profile","compliment_cute","compliment_list",
    #         "compliment_note","compliment_plain","compliment_cool",
    #         "compliment_funny","compliment_writer","compliment_photos"
    #     ],
    #     "json_fields": [
    #         "user_id","name","review_count","yelping_since","useful","funny",
    #         "cool","fans","average_stars","compliment_hot","compliment_more",
    #         "compliment_profile","compliment_cute","compliment_list",
    #         "compliment_note","compliment_plain","compliment_cool",
    #         "compliment_funny","compliment_writer","compliment_photos"
    #     ],
    #     "casts": {
    #         "review_count": int,
    #         "useful": int,
    #         "funny": int,
    #         "cool": int,
    #         "fans": int,
    #         "average_stars": float
    #     }
    # },
    # "stg.stg_tip": {
    #     "file": "data/subsampling/tip.json",
    #     "columns": ["user_id","business_id","text","tip_date","compliment_count"],
    #     "json_fields": ["user_id","business_id","text","date","compliment_count"],
    #     "casts": {"compliment_count": int}
    # },
    # "stg.stg_checkin": {
    #     "file": "data/subsampling/checkin.json",
    #     "columns": ["business_id","checkin_date"],
    #     "json_fields": ["business_id","date"],
    #     "casts": {}
    # }
}

# load raw data vào stg
def load_json_to_stg(conn, table_name, mapping, batch_size=1000):
    file_path = mapping["file"]
    cols = mapping["columns"]
    fields = mapping["json_fields"]
    casts = mapping.get("casts", {})
    
    try:
      with conn.cursor() as cur, open(file_path, "r", encoding="utf-8") as f:
          batch = []
          for line in f:
              row = json.loads(line)
              values = []
              for col, field in zip(cols, fields):
                  val = row.get(field)
                  if val is not None and col in casts:
                      try:
                          val = casts[col](val)
                      except Exception:
                          pass
                  values.append(val)
              batch.append(tuple(values))

              if len(batch) >= batch_size:
                  insert_batch(cur, table_name, cols, batch)
                  batch = []
          if batch:
              insert_batch(cur, table_name, cols, batch)
      conn.commit()
      print(f"✅ Loaded {file_path} → {table_name}")
    except Exception as e:
      conn.rollback()
      conn.close()
      print("Lỗi ở file load_stg.py:", e)
      return None


def insert_batch(cur, table, cols, batch):
    sql = f"""
    INSERT INTO {table} ({",".join(cols)})
    VALUES %s
    """
    psycopg2.extras.execute_values(cur, sql, batch)



if __name__ == "__main__":
  conn = get_connection('postgres')
  for table, mapping in MAPPINGS.items():
      load_json_to_stg(conn, table, mapping)
  conn.close()
