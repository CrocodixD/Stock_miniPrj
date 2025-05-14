import os
import yfinance as yf
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs

# 1. CẤU HÌNH CREDENTIALS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"./bucketKey.json"

# 2. KẾT NỐI GCS CHO ACTIONS
gcs_act, base_act = fs.FileSystem.from_uri(
    "gs://str-it4931/stock_data/actions/ticker=AAPL"
)

# 3. TẢI DỮ LIỆU CỔ TỨC & SPLITS
ticker = yf.Ticker("AAPL")
df_act = (
    ticker.actions
          .reset_index()
          .rename(columns={"Date": "date"})
)

# 4. Thêm cột partition theo năm/tháng
df_act["year"]  = df_act["date"].dt.year
df_act["month"] = df_act["date"].dt.month

# 5. Ghi Parquet partitioned
tbl_act = pa.Table.from_pandas(df_act, preserve_index=False)
pq.write_to_dataset(
    tbl_act,
    root_path=base_act,
    filesystem=gcs_act,
    partition_cols=["year", "month"],
    existing_data_behavior="overwrite_or_ignore"
)

print(f"✅ Đã ghi monthly actions AAPL lên gs://{base_act}/")