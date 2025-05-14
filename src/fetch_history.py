import os
import yfinance as yf
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs

# 1. CẤU HÌNH CREDENTIALS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"./bucketKey.json"

# 2. KẾT NỐI GCS CHO HISTORY
gcs_hist, base_hist = fs.FileSystem.from_uri(
    "gs://str-it4931/stock_data/history/ticker=AAPL"
)

# 3. TẢI TOÀN BỘ LỊCH SỬ GIÁ AAPL
ticker = yf.Ticker("AAPL")
df_hist = (
    ticker.history(period="max")     
          .reset_index()              
          .rename(columns={"Date": "date"})
)

# 4. Thêm cột partition theo năm/tháng
df_hist["year"]  = df_hist["date"].dt.year
df_hist["month"] = df_hist["date"].dt.month

# 5. Ghi Parquet phân vùng
tbl_hist = pa.Table.from_pandas(df_hist, preserve_index=False)
pq.write_to_dataset(
    tbl_hist,
    root_path=base_hist,                  
    filesystem=gcs_hist,
    partition_cols=["year", "month"],
    existing_data_behavior="overwrite_or_ignore"
)

print(f"✅ Đã ghi toàn bộ history AAPL lên gs://{base_hist}/")
