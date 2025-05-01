from google.cloud import storage
from google.oauth2 import service_account

# Đường dẫn đến file JSON key
key_path = r'./bucketKey.json'

# Tên bucket và tên object test
bucket_name = 'str-it4931'
test_blob_name = 'test_permission_check.txt'
test_content = 'This is a permission test.'

# Tạo client với Service Account
credentials = service_account.Credentials.from_service_account_file(key_path)
client = storage.Client(credentials=credentials)

try:
    # Lấy bucket
    bucket = client.get_bucket(bucket_name)

    # Ghi file lên bucket
    blob = bucket.blob(test_blob_name)
    blob.upload_from_string(test_content)
    print('✅ Ghi thành công lên bucket.')

    # Đọc file từ bucket
    downloaded = blob.download_as_text()
    print('✅ Đọc thành công:', downloaded)

    # (Tùy chọn) Xóa file test
    blob.delete()
    print('🧹 Đã xóa file test khỏi bucket.')

except Exception as e:
    print('❌ Lỗi khi kiểm tra quyền truy cập:', str(e))
