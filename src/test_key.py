import os
from pyarrow import fs

# Bước 1: Thiết lập biến môi trường credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"./bucketKey.json"

# Bước 2: Tạo GCS filesystem
gcs = fs.GcsFileSystem()

# Bước 3: Sử dụng `gcs` trong dataset
import pyarrow.dataset as ds

path_history = "str-it4931/stock_data/history/ticker=AAPL"

dataset = ds.dataset(
    path_history,
    filesystem=gcs,
    format="parquet",
    partitioning="hive"
)

table = dataset.to_table(
    filter=(ds.field("year") == 2024) & (ds.field("month").isin([3, 4]))
)
df = table.to_pandas()
print(df.head(100))
