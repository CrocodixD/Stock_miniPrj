import yfinance as yf

tsla = yf.Ticker("TSLA")
profile = tsla.info  # Thông tin cơ bản về công ty
print(profile)
