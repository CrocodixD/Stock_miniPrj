import os
import yfinance as yf
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs

# 1. CẤU HÌNH CREDENTIALS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"./bucketKey.json"

# 2. KẾT NỐI GCS cho fundamentals/gold
gcs, base_path = fs.FileSystem.from_uri(
    "gs://str-it4931/stock_data/fundamentals/gold"
)

# 3. TẢI TOÀN BỘ LỊCH SỬ GIÁ VÀNG
gold = yf.download("GC=F", period="max", interval="1d")
gold = gold.reset_index().rename(columns={"Date": "date"})

# 4. TÍNH GIÁ ĐÓNG CỬA TRUNG BÌNH THEO THÁNG (dùng 'ME' - month end)
gold.set_index("date", inplace=True)
monthly_ser = gold["Close"].resample("ME").mean()

# 5. Chuyển Series thành DataFrame và đặt tên cột
monthly = monthly_ser.reset_index().rename(columns={"Close": "monthlyPrice"})

# 6. THÊM CỘT partition: year, month
monthly["year"]  = monthly["date"].dt.year
monthly["month"] = monthly["date"].dt.month

# 7. CHUYỂN sang pyarrow Table
tbl = pa.Table.from_pandas(monthly, preserve_index=False)

# 8. GHI Parquet phân vùng year/month vào GCS
pq.write_to_dataset(
    tbl,
    root_path=base_path,
    filesystem=gcs,
    partition_cols=["year", "month"],
    existing_data_behavior="overwrite_or_ignore"
)

print(f"✅ Đã ghi monthly avg_close vàng lên gs://{base_path}/")
