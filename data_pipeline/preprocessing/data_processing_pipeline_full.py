# -*- coding: utf-8 -*-

"""
Data Processing Pipeline (Refactored)
=====================================
Tổng hợp code từ các notebook, giữ nguyên thứ tự:
1. Preprocessing Columns
2. Data Exploration Part 1
3. Data Exploration Part 2
4. Feature Filtering
5. Processing Functions

Đã loại bỏ tất cả các đoạn mã trùng lặp và gom luồng xử lý về một khối duy nhất.
Đã phục hồi toàn bộ các phân tích thống kê bị lược bỏ.
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import csv

# Cài đặt hiển thị
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.max_colwidth', None)

# Đường dẫn dữ liệu (có thể chỉnh sửa theo máy)
RAW_PATH = "/Users/nthanhdat/Documents/Fpt_Polytechnic/Graduation_Project/data_pipeline/raw_data"

# Hàm tải & xử lý dữ liệu

def load_and_prepare(files, drop_airline=False, drop_is_duplicate=False):
    dfs = []
    for file in files:
        df = pd.read_csv(os.path.join(RAW_PATH, file))
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        if drop_airline and "airline" in df.columns:
            df.drop(columns="airline", inplace=True)
        if drop_is_duplicate and "is_duplicate" in df.columns:
            df.drop(columns="is_duplicate", inplace=True)
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

# Load dữ liệu từ 3 hãng bay
df_vietjet = load_and_prepare(["vietjet_1.csv", "vietjet_2.csv"], drop_airline=True, drop_is_duplicate=True)
df_vnairlines = load_and_prepare(["VNairlines_1_1.csv", "VNairlines_2_2.csv"], drop_airline=True)
df_bamboo = load_and_prepare(["BambooAirway.csv"])

# Gộp tất cả vào 1 DataFrame
df_all = pd.concat([df_vietjet, df_vnairlines, df_bamboo], ignore_index=True)

# In thông tin tổng quan
print("Tổng số dòng sau khi gộp:", len(df_all))
print("Số lượng hãng bay duy nhất:", df_all['operating_airline_name'].nunique())

# Loại bỏ các cột không cần thiết
cols_to_drop = ['source.name', 'departure_time', 'arrival_time', 'status']
df_all.drop(columns=cols_to_drop, inplace=True, errors='ignore')

# Ép kiểu datetime
df_all['date'] = pd.to_datetime(df_all['date'], errors='coerce')
df_all['hour'] = df_all['date'].dt.hour

# Tạo cột thời gian theo khung giờ

def get_time_slot(hour):
    if 5 <= hour < 12:
        return 'Sáng'
    elif 12 <= hour < 17:
        return 'Chiều'
    elif 17 <= hour < 21:
        return 'Tối'
    else:
        return 'Đêm'

df_all['time_slot'] = df_all['hour'].apply(get_time_slot)
df_all['month'] = df_all['date'].dt.month
df_all['weekday'] = df_all['date'].dt.day_name()
df_all['week'] = df_all['date'].dt.isocalendar().week

# Tag ngày lễ
holidays = pd.to_datetime([
    '2025-01-01', '2025-02-10', '2025-02-11', '2025-02-12', '2025-02-13',
    '2025-02-14', '2025-02-15', '2025-02-16', '2025-04-30', '2025-05-01', '2025-09-02'
])
df_all['tag_ngay'] = df_all['date'].apply(lambda x: 'Ngày lễ' if x in holidays else 'Ngày bình thường')

# Thay route bằng tên đầy đủ
airport_dict = {
    'DAD': 'Sân bay quốc tế Đà Nẵng', 'HAN': 'Sân bay quốc tế Nội Bài', 'HPH': 'Sân bay quốc tế Cát Bi',
    'SGN': 'Sân bay quốc tế Tân Sơn Nhất', 'CXR': 'Sân bay quốc tế Cam Ranh', 'DLI': 'Sân bay Liên Khương',
    'HUI': 'Sân bay Phú Bài', 'PQC': 'Sân bay quốc tế Phú Quốc', 'UIH': 'Sân bay Phù Cát'
}

def replace_route_with_names(route):
    try:
        dep, arr = route.split('_')
        return f"{airport_dict.get(dep, dep)} - {airport_dict.get(arr, arr)}"
    except:
        return route

df_all['description'] = df_all['route'].apply(replace_route_with_names)

# Tính các chỉ số giá vé
df_all['net_price'] = df_all['price_vnd'] - df_all['taxes']
df_all['tax_ratio'] = df_all['taxes'] / df_all['price_vnd']

# Phân loại loại vé
def infer_fare_type(row):
    code = str(row['fare_code']).upper()
    cond = str(row['fare_conditions']).upper()
    if any(k in cond for k in ['BUS', 'PRE33', 'PEN33']) or code.startswith('B'):
        return 'Business'
    elif any(k in cond for k in ['PRE', 'FLE']) or code.startswith('E'):
        return 'Premium'
    elif any(k in cond for k in ['PEN', 'ECO']) or code.startswith('Y') or code.startswith('Z'):
        return 'Economy'
    else:
        return 'Không rõ'

df_all['fare_type'] = df_all.apply(infer_fare_type, axis=1)

# Tính độ biến động giá
price_std = df_all.groupby('route')['price_vnd'].std()
df_all['price_std'] = df_all['route'].map(price_std)
df_all['price_volatility'] = pd.qcut(df_all['price_std'], q=3, labels=['Ổn định', 'Trung bình', 'Biến động cao'])

# ===================== PHÂN TÍCH DỮ LIỆU MỞ RỘNG =====================

print("\n================= PHÂN TÍCH TỔNG QUAN =================")
print("Tổng số chuyến bay:", len(df_all))
print("\nSố chuyến theo hãng:")
print(df_all['operating_airline_name'].value_counts())
print("\nThống kê giá vé (price_vnd):")
print(df_all['price_vnd'].describe())
print("\nGiá vé trung bình theo tháng:")
print(df_all.groupby('month')['price_vnd'].mean().round(0))
print("\nGiá vé trung bình theo ngày trong tuần:")
print(df_all.groupby('weekday')['price_vnd'].mean().round(0))
print("\nGiá vé trung bình theo tuần:")
print(df_all.groupby('week')['price_vnd'].mean().round(0))
print("\n[1] Tổng số chuyến theo hãng và ngày:")
print(df_all.groupby(['date', 'operating_airline_name']).size().unstack(fill_value=0).head())
print("\n[2] Tổng số chuyến theo tuyến bay:")
print(df_all['route'].value_counts().head())
print("\n[3] Tỷ lệ loại vé theo time_slot:")
time_slot_ratio = df_all.groupby(['time_slot', 'fare_type']).size().unstack(fill_value=0)
time_slot_ratio_percent = (time_slot_ratio.T / time_slot_ratio.sum(axis=1)).T
print(time_slot_ratio_percent.round(2))
print("\n[4] Xu hướng giá vé theo thời gian:")
daily_avg_price = df_all.groupby('date')['price_vnd'].mean().rolling(7).mean()
print(daily_avg_price.dropna().head())
print("\n[5] Top sân bay xuất phát có số chuyến nhiều nhất:")
df_all['from_airport'] = df_all['route'].str.split('_').str[0]
print(df_all['from_airport'].value_counts().head())
print("\n[6] Tuyến bay có độ biến động giá cao nhất:")
print(df_all[['route', 'price_std']].drop_duplicates().sort_values('price_std', ascending=False).head())
print("\n[7] So sánh giá trung bình giữa ngày lễ và ngày thường:")
print(df_all.groupby('tag_ngay')['price_vnd'].mean().round())
print("\n[8] Giá trung bình theo Cabin:")
cabin_avg = df_all.groupby('cabin')['price_vnd'].mean()
print(cabin_avg.apply(lambda x: f"{x:,.0f} VND"))
print("\n[9] So sánh giá giữa các hãng cùng tuyến theo Cabin:")
cabin_airline = df_all.groupby(['route', 'cabin', 'operating_airline_name'])['price_vnd'].mean().reset_index()
pivot_compare = cabin_airline.pivot_table(index=['route', 'cabin'], columns='operating_airline_name', values='price_vnd')
print(pivot_compare.applymap(lambda x: f"{x:,.0f} VND" if pd.notnull(x) else "-"))
print("\n[10] Hãng có giá tốt nhất theo tuyến và hạng vé:")
def best_price(row):
    vals = row.dropna()
    return vals.idxmin() if len(vals) >= 2 else None
pivot_compare['Best_price_airline'] = pivot_compare.apply(best_price, axis=1)
print(pivot_compare[['Best_price_airline']])
print("\n[11] Phân tích độ linh hoạt Fare:")
flex = df_all.groupby(['booking_class', 'fare_code', 'fare_conditions'])['price_vnd'].agg(['mean', 'count'])
print(flex.sort_values(by='mean').head(10))
print("\n[12] Số chuyến bay theo ngày:")
daily = df_all['date'].value_counts().sort_values(ascending=False)
print(daily.head(10))
print("\n[13] Tuyến bay có thuế cao nhất:")
avg_tax = df_all.groupby('route')['taxes'].mean().sort_values(ascending=False)
print(avg_tax.head(5))
print("\n[14] Giá cao nhất theo loại vé và hãng:")
max_price = df_all.groupby(['operating_airline_name', 'fare_type'])['price_vnd'].max()
print(max_price)

# Fix cột số dài làm tròn 
df_all['tax_ratio'] = df_all['tax_ratio'].round(3)
df_all['price_std'] = df_all['price_std'].round(0)  # làm tròn nguyên
df_all['net_price'] = df_all['net_price'].round(0)


# Lưu kết quả ra file CSV
OUTPUT_PATH = "/Users/nthanhdat/Documents/Fpt_Polytechnic/Graduation_Project/data_pipeline/raw_data/data_full"
os.makedirs(OUTPUT_PATH, exist_ok=True)  # Tạo thư mục nếu chưa có

output_file = os.path.join(OUTPUT_PATH, "data_full.csv")
df_all.to_csv(output_file, index=False, encoding='utf-8-sig', sep=',', quoting=csv.QUOTE_MINIMAL)

print(f"\n✅ File đã được lưu thành công tại: {output_file}")
