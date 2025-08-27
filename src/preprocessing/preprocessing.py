import os
import pandas as pd
from config.settings import BASE_DIR,DATA_RAW_DIR, DATA_SUBSAMPLING_DIR
from src.utils.db import get_connection
import pandas as pd
import json

def subsampling():
  print('123')
  sample_size = 100000
  input_data_path = os.path.join(DATA_RAW_DIR,'raw' , 'yelp_academic_dataset_review.json')
  output_data_path = os.path.join(DATA_RAW_DIR, 'subsampling', 'review.json')
  
  os.makedirs(os.path.join(DATA_RAW_DIR, 'subsampling') , exist_ok=True)
  
  with open(input_data_path, "r") as f_in, open(output_data_path, "w") as f_out:
      for i, line in enumerate(f_in):
          if i < sample_size:
              f_out.write(line)
          else:
              break

def filter_user():
  user_ids = set()
  for file in ["review.json", "yelp_academic_dataset_tip.json"]:
      input_data_path = os.path.join(DATA_RAW_DIR,'subsampling' , file)
      with open(input_data_path, "r") as f:
          for line in f:
              data = json.loads(line)
              user_ids.add(data["user_id"])
  
  with open(os.path.join(DATA_RAW_DIR, 'raw/yelp_academic_dataset_user.json'), "r") as fin, open(os.path.join(DATA_RAW_DIR, 'subsampling/user.json'), "w") as fout:
    for line in fin:
        user = json.loads(line)
        if user["user_id"] in user_ids:
            fout.write(line)

if __name__ == '__main__':
  # extract_file('review.json')
  # subsampling()
  filter_user()
  