from confluent_kafka import Consumer, KafkaError
import json
import sys

# --- Cấu hình Kafka Consumer ---
consumer_conf = {
    'bootstrap.servers': '192.168.1.239:9092',  # Địa chỉ broker, khớp với producer
    'group.id': 'stock_ohlcv_consumer_group',   # Consumer group ID
    'auto.offset.reset': 'earliest'             # Đọc từ đầu topic nếu không có offset
}

# --- Khởi tạo consumer ---
consumer = Consumer(consumer_conf)

# --- Subscribe vào topic ---
topic = 'stock_ohlcv'
consumer.subscribe([topic])

print(f"[INFO] Consumer đã subscribe vào topic {topic}. Đang chờ message...")

# --- Đọc và in dữ liệu ---
try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Chờ message trong 1 giây
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Đạt đến cuối partition, không phải lỗi
                print(f"[INFO] Đã đọc hết message trong partition {msg.partition()}")
            else:
                print(f"[ERROR] Consumer error: {msg.error()}")
            continue

        # Lấy dữ liệu từ message
        key = msg.key().decode('utf-8') if msg.key() else None
        value = msg.value().decode('utf-8') if msg.value() else None
        partition = msg.partition()
        offset = msg.offset()

        # Parse JSON từ value
        try:
            data = json.loads(value)
            print(f"\n[CONSUMER] Message nhận được:")
            print(f"  Topic: {msg.topic()}")
            print(f"  Partition: {partition}")
            print(f"  Offset: {offset}")
            print(f"  Key: {key}")
            print(f"  Value: {json.dumps(data, indent=2)}")
        except json.JSONDecodeError as e:
            print(f"[ERROR] Không thể parse JSON từ message: {e}")
            print(f"  Raw value: {value}")

except KeyboardInterrupt:
    print("\n[INFO] Consumer bị dừng bởi người dùng")
finally:
    consumer.close()