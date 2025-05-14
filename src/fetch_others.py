import os
import yfinance as yf
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs

# ── 1. CREDENTIALS & GCS FS ─────────────────────────────────────────────────────
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"./bucketKey.json"
gcs = fs.GcsFileSystem()

# ── 2. TẠO OBJECT TICKER ───────────────────────────────────────────────────────
ticker = yf.Ticker("AAPL")

# ── 3. INFO (static metadata) ─────────────────────────────────────────────────
base_info = "str-it4931/stock_data/info/ticker=AAPL"
df_info = pd.DataFrame([ticker.info])
tbl_info = pa.Table.from_pandas(df_info, preserve_index=False)
pq.write_table(
    tbl_info,
    f"{base_info}/info.parquet",
    filesystem=gcs
)
print(f"✅ info written to gs://{base_info}/info.parquet")

# ── 4. FINANCIALS (annual) ────────────────────────────────────────────────────
base_fin = "str-it4931/stock_data/fundamentals/financials/ticker=AAPL"
df_fin = (
    ticker.financials
          .T
          .reset_index()
          .rename(columns={"index": "period"})
)
df_fin["year"] = df_fin["period"].dt.year.astype(str)
tbl_fin = pa.Table.from_pandas(df_fin, preserve_index=False)
pq.write_to_dataset(
    tbl_fin,
    root_path=base_fin,
    filesystem=gcs,
    partition_cols=["year"],
    existing_data_behavior="overwrite_or_ignore"
)
print(f"✅ financials written under gs://{base_fin}/")

# ── 5. QUARTERLY FINANCIALS ─────────────────────────────────────────────────
base_qfin = "str-it4931/stock_data/fundamentals/quarterly_financials/ticker=AAPL"
df_qfin = (
    ticker.quarterly_financials
          .T
          .reset_index()
          .rename(columns={"index": "period"})
)
df_qfin["year"]    = df_qfin["period"].dt.year.astype(str)
df_qfin["quarter"] = df_qfin["period"].dt.quarter.map(lambda q: f"Q{q}")
tbl_qfin = pa.Table.from_pandas(df_qfin, preserve_index=False)
pq.write_to_dataset(
    tbl_qfin,
    root_path=base_qfin,
    filesystem=gcs,
    partition_cols=["year", "quarter"],
    existing_data_behavior="overwrite_or_ignore"
)
print(f"✅ quarterly_financials written under gs://{base_qfin}/")

# ── 6. ANALYST PRICE TARGETS ─────────────────────────────────────────────────
base_apt = "str-it4931/stock_data/fundamentals/analyst_price_targets/ticker=AAPL"
apt_raw   = ticker.analyst_price_targets 
df_apt = pd.DataFrame([apt_raw])
tbl_apt = pa.Table.from_pandas(df_apt, preserve_index=False)
pq.write_table(
    tbl_apt,
    f"{base_apt}/analyst_price_targets.parquet",
    filesystem=gcs
)
print(f"✅ analyst_price_targets written under gs://{base_apt}/analyst_price_targets.parquet")

# ── 7. INCOME STATEMENT ────────────────────────────────────────────────────────
base_income = "str-it4931/stock_data/fundamentals/income_statements/ticker=AAPL"
df_income = ticker.get_income_stmt()
if df_income is not None and not df_income.empty:
    df_income = df_income.reset_index()
    tbl_income = pa.Table.from_pandas(df_income, preserve_index=False)
    pq.write_table(
        tbl_income,
        f"{base_income}/income_statement.parquet",
        filesystem=gcs
    )
    print(f"✅ income_statement written under gs://{base_income}/")
else:
    print("⚠️ No income_statement data available.")

# ── 8. EARNINGS HISTORY ───────────────────────────────────────────────────────
base_ehist = "str-it4931/stock_data/fundamentals/earnings_history/ticker=AAPL"
df_ehist = ticker.get_earnings_history()
# df_ehist có thể là DataFrame với index 'quarter'
if isinstance(df_ehist, pd.DataFrame) and not df_ehist.empty:
    df_ehist = df_ehist.reset_index().rename(columns={df_ehist.index.name: 'period'})
    tbl_ehist = pa.Table.from_pandas(df_ehist, preserve_index=False)
    pq.write_table(
        tbl_ehist,
        f"{base_ehist}/earnings_history.parquet",
        filesystem=gcs
    )
    print(f"✅ earnings_history written under gs://{base_ehist}/")
else:
    print("⚠️ No earnings_history data available.")

# ── 9. EARNINGS ESTIMATE ──────────────────────────────────────────────────────
base_eest = "str-it4931/stock_data/fundamentals/earnings_estimates/ticker=AAPL"
df_eest = ticker.get_earnings_estimate()
# df_eest có thể là DataFrame với index 'period'
if isinstance(df_eest, pd.DataFrame) and not df_eest.empty:
    df_eest = df_eest.reset_index().rename(columns={df_eest.index.name: 'period'})
    tbl_eest = pa.Table.from_pandas(df_eest, preserve_index=False)
    pq.write_table(
        tbl_eest,
        f"{base_eest}/earnings_estimates.parquet",
        filesystem=gcs
    )
    print(f"✅ earnings_estimates written under gs://{base_eest}/")
else:
    print("⚠️ No earnings_estimates data available.")