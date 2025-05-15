import pandas as pd
import time
import os
from fetch_actions import fetch_actions
from fetch_history import fetch_history
from fetch_others import fetch_others

# Đường dẫn đến file ghi nhận tiến trình
progress_file = "progress.txt"

# Đọc file CSV chứa danh sách mã stock
df_stocks = pd.read_csv("stockList.csv")

# Kiểm tra xem file tiến trình có tồn tại không
if os.path.exists(progress_file):
    with open(progress_file, "r") as f:
        last_index = int(f.read().strip())
else:
    last_index = -1  # Bắt đầu từ đầu nếu không có file tiến trình

# Lặp qua từng mã stock từ index tiếp theo
for index, row in df_stocks.iloc[last_index + 1:].iterrows():
    ticker_symbol = row['Symbol']
    print(f"Processing {ticker_symbol} (index {index})...")
    
    try:
        # Gọi các hàm fetch
        fetch_actions(ticker_symbol)
        fetch_history(ticker_symbol)
        fetch_others(ticker_symbol)
        
        # Ghi index hiện tại vào file tiến trình sau khi xử lý thành công
        with open(progress_file, "w") as f:
            f.write(str(index))
        
        # Thêm khoảng nghỉ giữa các mã stock để tránh rate limit
        time.sleep(10)
    
    except Exception as e:
        # Ghi lỗi ra console và tiếp tục với mã stock tiếp theo
        print(f"Error processing {ticker_symbol}: {e}")
        continue