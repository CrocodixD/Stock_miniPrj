import os
import pandas as pd
import pyarrow.parquet as pq
from pyarrow import fs

# 1. Thiết lập credentials và GCS filesystem
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"./bucketKey.json"
gcs = fs.GcsFileSystem()

# 2. Định nghĩa list các đường dẫn cần kiểm tra
paths = {
    "info":                         "str-it4931/stock_data/info/ticker=AAPL/info.parquet",
    "financials (annual)":          "str-it4931/stock_data/fundamentals/financials/ticker=AAPL",
    "quarterly_financials":         "str-it4931/stock_data/fundamentals/quarterly_financials/ticker=AAPL",
    "analyst_price_targets":        "str-it4931/stock_data/fundamentals/analyst_price_targets/ticker=AAPL/analyst_price_targets.parquet",
    "income_statement":             "str-it4931/stock_data/fundamentals/income_statements/ticker=AAPL/income_statement.parquet",
    "earnings_history":             "str-it4931/stock_data/fundamentals/earnings_history/ticker=AAPL/earnings_history.parquet",
    "earnings_estimates":           "str-it4931/stock_data/fundamentals/earnings_estimates/ticker=AAPL/earnings_estimates.parquet",
    "actions":                       "str-it4931/stock_data/actions/ticker=AAPL",
}

# 3. Hàm phụ để đọc và in preview
def preview_parquet(path, partitioned=False, n=5):
    print(f"\n=== Preview: {path} ===")
    if partitioned:
        # đọc cả thư mục dataset
        dataset = pq.ParquetDataset(path, filesystem=gcs)
        table = dataset.read()
    else:
        table = pq.read_table(path, filesystem=gcs)
    df = table.to_pandas()
    print(df.head(n))

# 4. Chạy preview cho từng loại dữ liệu
#   với các dataset annual/quarterly partitioned, set partitioned=True
preview_parquet(paths["info"])

preview_parquet(paths["financials (annual)"], partitioned=True)

preview_parquet(paths["quarterly_financials"], partitioned=True)

preview_parquet(paths["analyst_price_targets"])

preview_parquet(paths["income_statement"])

preview_parquet(paths["earnings_history"])

preview_parquet(paths["earnings_estimates"])

preview_parquet(paths["actions"])
