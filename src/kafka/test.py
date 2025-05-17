import yfinance as yf
import pandas as pd
import json
from datetime import datetime, timedelta
from confluent_kafka import Producer
import time

# --- Cấu hình Kafka ---
producer_conf = {'bootstrap.servers': '192.168.1.239:9092'}
producer = Producer(producer_conf)

# --- Hàm callback xác nhận gửi dữ liệu ---
def delivery_report(err, msg):
    if err is not None:
        print(f"[KAFKA ERROR] Message delivery failed: {err}")
    else:
        print(f"[KAFKA OK] Delivered to {msg.topic()} [{msg.partition()}]")

# --- Danh sách mã cổ phiếu ---
ticker_list = ['AAPL'] # , 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'NVDA', 'NFLX', 'ADBE', 'INTC']
topic = 'stock_ohlcv'

while True:
    print(f"\n[INFO] Bắt đầu chạy vào {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    end_date = datetime.today()
    start_date = end_date - timedelta(days=30)

    for ticker_symbol in ticker_list:
        print(f"[INFO] Lấy dữ liệu cho: {ticker_symbol}")
        try:
            df = yf.download(
                ticker_symbol,
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d'),
                auto_adjust=False,
                progress=False
            )

            if df.empty:
                print(f"[WARNING] Không có dữ liệu cho {ticker_symbol}")
                continue

            # Debug: In cấu trúc DataFrame
            print(f"[DEBUG] Columns before reset_index: {df.columns}")
            df.reset_index(inplace=True)
            print(f"[DEBUG] Columns after reset_index: {df.columns}")
            if not df.empty:
                print(f"[DEBUG] First row: {df.iloc[0].to_dict()}")

            for _, row in df.iterrows():
                record = {
                    'ticker': ticker_symbol,
                    'date': row[('Date', '')].strftime('%Y-%m-%d'),
                    'open': row[('Open', ticker_symbol)],
                    'high': row[('High', ticker_symbol)],
                    'low': row[('Low', ticker_symbol)],
                    'close': row[('Close', ticker_symbol)],
                    'volume': row[('Volume', ticker_symbol)]
                }

                json_value = json.dumps(record)
                producer.produce(
                    topic=topic,
                    key=ticker_symbol,
                    value=json_value,
                    callback=delivery_report
                )
                producer.poll(0)

            print(f"[OK] Đã gửi dữ liệu cho {ticker_symbol}")

        except Exception as e:
            print(f"[ERROR] Lỗi khi lấy/gửi {ticker_symbol}: {e}")

    producer.flush()
    print(f"[INFO] Đã hoàn tất 1 vòng gửi dữ liệu lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print("[WAIT] Đợi 24h để chạy lại...\n")
    time.sleep(86400)