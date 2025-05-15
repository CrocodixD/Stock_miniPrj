import os
import pandas as pd
import pyarrow.parquet as pq
from pyarrow import fs

# 1. Thiết lập credentials và GCS filesystem
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"./bucketKey.json"
gcs = fs.GcsFileSystem()

# 2. Đường dẫn tới dataset gold partitioned
gold_path = "str-it4931/stock_data/fundamentals/gold"

# 3. Hàm preview cho dataset partitioned
def preview_partitioned_dataset(path, n=5):
    print(f"\n=== Preview partitioned dataset: {path} ===")
    # Dùng ParquetDataset để tự động đọc tất cả file trong thư mục con
    dataset = pq.ParquetDataset(path, filesystem=gcs)
    table = dataset.read()
    df = table.to_pandas()
    print(f"Total rows: {len(df)}")
    print("\n-- First rows --")
    print(df.head(n).to_string(index=False))
    print("\n-- Last rows --")
    print(df.tail(n).to_string(index=False))

# 4. Chạy preview
preview_partitioned_dataset(gold_path)
