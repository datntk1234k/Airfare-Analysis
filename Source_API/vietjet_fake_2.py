import pandas as pd
import random
import string
from datetime import datetime, timedelta

# Đọc dữ liệu từ file CSV
vn_data = pd.read_csv('/Users/nthanhdat/Documents/Fpt_Polytechnic/Graduation_Project/data_pipeline/raw_data/VNairlines_2_2.csv')

# Danh sách sân bay hợp lệ
valid_airports = {'Hà Nội': 'HAN', 'Sài Gòn': 'SGN', 'Đà Nẵng': 'DAD', 'Huế': 'HUI', 
                  'Nha Trang': 'CXR', 'Hải Phòng': 'HPH', 'Đà Lạt': 'DLI'}

# Lọc dữ liệu chỉ giữ các tuyến bay hợp lệ
def filter_valid_routes(row):
    origin = row['Origin']
    dest = row['Destination']
    return origin in valid_airports.values() and dest in valid_airports.values()

vn_data = vn_data[vn_data.apply(filter_valid_routes, axis=1)].copy()

# Xác định các ngày lễ lớn trong khoảng 1/6/2025 - 1/6/2026
holidays = [
    (datetime(2026, 1, 1), datetime(2026, 1, 7)),  # Tết (ước lượng)
    (datetime(2025, 4, 30), datetime(2025, 5, 3)),  # 30/4 - 1/5
    (datetime(2025, 9, 1), datetime(2025, 9, 3))   # Quốc khánh (2/9)
]

# Hàm kiểm tra ngày có phải ngày lễ không
def is_holiday(date_str):
    date = datetime.strptime(date_str, '%m/%d/%Y')
    for start, end in holidays:
        if start <= date <= end:
            return True
    return False

# Tạo dữ liệu cho Vietjet
vietjet_data = vn_data.copy()

# Giảm giá vé và thuế ngẫu nhiên từ 40% đến 60% và làm tròn
discount = random.uniform(0.4, 0.6)
vietjet_data['Price_VND'] = (vietjet_data['Price_VND'] * (1 - discount)).astype(int)
vietjet_data['Taxes'] = (vietjet_data['Taxes'] * (1 - discount)).astype(int)

# Cập nhật hãng hàng không
vietjet_data['Operating_Airline_Code'] = 'VJ'
vietjet_data['Operating_Airline_Name'] = 'Vietjet Air'

# Tạo Flight_ID mới
def generate_flight_id(origin, dest, date, is_holiday):
    segment = 'SEG'
    flight_code = 'VJ' + ''.join(random.choices(string.digits, k=3))
    route = f"{origin}{dest}"
    flight_count = 2 if is_holiday else 1  # Gấp đôi chuyến bay dịp lễ
    return f"{segment}-{flight_code}-{route}-2025-{flight_count}-01-1450"

vietjet_data['Flight_ID'] = vietjet_data.apply(
    lambda row: generate_flight_id(row['Origin'], row['Destination'], row['Date'], is_holiday(row['Date'])), axis=1
)

# Lưu dữ liệu giả sang file mới
vietjet_data.to_csv('/Users/nthanhdat/Documents/Fpt_Polytechnic/Graduation_Project/dataset/Test_1/vietjet_data2.csv', index=False)