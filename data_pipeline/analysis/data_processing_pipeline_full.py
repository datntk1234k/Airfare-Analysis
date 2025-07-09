# -*- coding: utf-8 -*-

"""
Data Processing Pipeline
========================
Tổng hợp đầy đủ code từ 4 notebook gốc, sắp xếp theo luồng xử lý:
1. Tiền xử lý & xoá cột dư thừa.
2. Khám phá dữ liệu.
3. Lọc & chọn feature.
Sau cùng, file sẽ lưu dataset đã xử lý ra `data/processed_data.parquet`.
"""


# === Preprocessing Columns (from 01_preprocessing_columns_full.ipynb) ===

import pandas as pd
import os

# Cài đặt hiển thị đẹp
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.max_colwidth', None)

# Đường dẫn thư mục chứa file
RAW_PATH = "D:\\E\\raw_data"

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

# 1. Vietjet: xoá airline & is_duplicate
df_vietjet = load_and_prepare(["vietjet_1.csv", "vietjet_2.csv"],
                              drop_airline=True, drop_is_duplicate=True)

# 2. Vietnam Airlines: xoá airline
df_vnairlines = load_and_prepare(["VNairlines_1_1.csv", "VNairlines_2_2.csv"],
                                 drop_airline=True)

# 3. Bamboo: giữ nguyên hoàn toàn
df_bamboo = load_and_prepare(["BambooAirway.csv"])

# Gộp tất cả
df_all = pd.concat([df_vietjet, df_vnairlines, df_bamboo], ignore_index=True)

# Xem kết quả
print("Tổng số dòng sau khi gộp:", len(df_all))
print(df_all.head(10))




# Kiểm tra có bao nhiêu hãng bay 
unique_airlines = df_all['operating_airline_name'].dropna().unique()
print("Số lượng hãng bay duy nhất trong 'operating_airline_name':", len(unique_airlines))
print("🛫 Danh sách các hãng bay:")
print(unique_airlines)




df_all.drop(columns=['source.name', 'departure_time', 'arrival_time', 'status'], inplace=True)
print("Đã xóa các cột: source.name, departure_time, arrival_time, status khỏi df_all.")





print("🧾 Các cột hiện có trong df_all:")
print(df_all.columns.tolist())




df_all.info()



# Ép kiểu cột date về datetime
df_all['date'] = pd.to_datetime(df_all['date'], errors='coerce')



# Tạo cột giờ
df_all['hour'] = df_all['date'].dt.hour

# Tạo cột "khoảng thời gian bay"
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



# Số chuyến bay tổng cộng
print("Tổng số chuyến bay:", len(df_all))

# Số chuyến theo hãng
print("\nSố chuyến theo hãng:")
print(df_all['operating_airline_name'].value_counts())

# Thống kê giá vé
print("\nThống kê giá vé (price_vnd):")
print(df_all['price_vnd'].describe())



# Đảm bảo date đã đúng kiểu datetime
df_all['date'] = pd.to_datetime(df_all['date'], errors='coerce')

# Thêm các cột thời gian
df_all['month'] = df_all['date'].dt.month
df_all['weekday'] = df_all['date'].dt.day_name()
df_all['week'] = df_all['date'].dt.isocalendar().week



print("\nGiá vé trung bình theo tháng:")
print(df_all.groupby('month')['price_vnd'].mean().round(0))



print("\nGiá vé trung bình theo ngày trong tuần:")
print(df_all.groupby('weekday')['price_vnd'].mean().round(0))




print("\nGiá vé trung bình theo tuần:")
print(df_all.groupby('week')['price_vnd'].mean().round(0))








# === Data Exploration ‒ Part 1 (from 02_data_exploration_part1.ipynb) ===

import pandas as pd
import os

# Cài đặt hiển thị đẹp
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.max_colwidth', None)

# Đường dẫn thư mục chứa file
RAW_PATH = "D:\\SUMMER2025\\raw_data"

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

# 1. Vietjet: xoá airline & is_duplicate
df_vietjet = load_and_prepare(["vietjet_1.csv", "vietjet_2.csv"],
                              drop_airline=True, drop_is_duplicate=True)

# 2. Vietnam Airlines: xoá airline
df_vnairlines = load_and_prepare(["VNairlines_1_1.csv", "VNairlines_2_2.csv"],
                                 drop_airline=True)

# 3. Bamboo: giữ nguyên hoàn toàn
df_bamboo = load_and_prepare(["BambooAirway.csv"])

# Gộp tất cả
df_all = pd.concat([df_vietjet, df_vnairlines, df_bamboo], ignore_index=True)

# Xem kết quả
print("Tổng số dòng sau khi gộp:", len(df_all))
print(df_all.head(10))



# Kiểm tra có bao nhiêu hãng bay 
unique_airlines = df_all['operating_airline_name'].dropna().unique()
print("Số lượng hãng bay duy nhất trong 'operating_airline_name':", len(unique_airlines))
print("🛫 Danh sách các hãng bay:")
print(unique_airlines)




df_all.drop(columns=['source.name', 'departure_time', 'arrival_time', 'status'], inplace=True)
print("Đã xóa các cột: source.name, departure_time, arrival_time, status khỏi df_all.")





print("🧾 Các cột hiện có trong df_all:")
print(df_all.columns.tolist())




df_all.info()



# Ép kiểu cột date về datetime
df_all['date'] = pd.to_datetime(df_all['date'], errors='coerce')



# Tạo cột giờ
df_all['hour'] = df_all['date'].dt.hour

# Tạo cột "khoảng thời gian bay"
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



# Đảm bảo date đã đúng kiểu datetime
df_all['date'] = pd.to_datetime(df_all['date'], errors='coerce')

# Thêm các cột thời gian
df_all['month'] = df_all['date'].dt.month
df_all['weekday'] = df_all['date'].dt.day_name()
df_all['week'] = df_all['date'].dt.isocalendar().week



df_all.info()
df_all




# Giả sử df_all là DataFrame đã được gộp sẵn từ các file
# Kiểm tra các cột có liên quan
print(df_all[['route', 'price_vnd', 'cabin', 'operating_airline_name', 'booking_class', 'fare_code', 'fare_conditions']].head())

# 1. So sánh chênh lệch giá theo hạng vé (Economy vs Business)
cabin_avg_price = df_all.groupby('cabin')['price_vnd'].mean().sort_values()
print("\n[1] Giá trung bình theo Cabin:\n", cabin_avg_price.apply(lambda x: f"{x:,.0f} VND"))

# 2. So sánh giá Economy/Business giữa các hãng cùng tuyến
cabin_airline_route = df_all.groupby(['route', 'cabin', 'operating_airline_name'])['price_vnd'].mean().reset_index()
pivot_price_compare = cabin_airline_route.pivot_table(index=['route', 'cabin'], 
                                                       columns='operating_airline_name', 
                                                       values='price_vnd')
print("\n[2] So sánh giá giữa các hãng trên cùng tuyến (theo Cabin):\n", pivot_price_compare.applymap(lambda x: f"{x:,.0f} VND" if pd.notnull(x) else "-"))







# 3. Xem hãng nào giá tốt hơn cho từng hạng
def better_price(row):
    values = row.dropna()
    if len(values) >= 2:
        return values.idxmin()
    return None

pivot_price_compare['Best_price_airline'] = pivot_price_compare.apply(better_price, axis=1)
print("\n[3] Hãng có giá tốt nhất theo từng tuyến và hạng:\n", pivot_price_compare[['Best_price_airline']])

# 4. Phân tích độ linh hoạt (Booking_Class, Fare_Code, Fare_Conditions)
flex_analysis = df_all.groupby(['booking_class', 'fare_code', 'fare_conditions'])['price_vnd'].agg(['mean', 'count']).sort_values(by='mean')
print("\n[4] Phân tích độ linh hoạt:\n", flex_analysis.head(10))  # hiển thị 10 dòng đầu



# Đảm bảo cột ngày là dạng datetime
df_all['date'] = pd.to_datetime(df_all['date'])

# Đếm số chuyến bay theo từng ngày
flights_per_day = df_all['date'].value_counts().sort_values(ascending=False)

# Hiển thị top 10 ngày có nhiều chuyến nhất
print("Top 10 ngày có nhiều chuyến bay nhất:")
print(flights_per_day.head(10))




# Trung bình giá vé theo tháng
month_avg_price = df_all.groupby('month')['price_vnd'].mean().sort_values(ascending=False)

print("\n[3] Giá vé trung bình theo tháng (giảm dần):\n")
print(month_avg_price.apply(lambda x: f"{x:,.0f} VND"))

# Tháng có giá vé trung bình cao nhất
max_month = month_avg_price.idxmax()
max_price = month_avg_price.max()

print(f"\nTháng có giá vé trung bình cao nhất: Tháng {max_month} với mức giá trung bình: {max_price:,.0f} VND")




# Trung bình giá vé theo tháng và hãng
monthly_airline_price = df_all.groupby(['operating_airline_name', 'month'])['price_vnd'].mean()

# Tìm tháng có giá cao nhất cho mỗi hãng
highest_month_per_airline = monthly_airline_price.groupby('operating_airline_name').idxmax()
highest_price_per_airline = monthly_airline_price.groupby('operating_airline_name').max()

# Gộp kết quả
result = pd.DataFrame({
    'Tháng cao nhất': highest_month_per_airline.map(lambda x: x[1]),
    'Giá trung bình cao nhất (VND)': highest_price_per_airline
})
result['Giá trung bình cao nhất (VND)'] = result['Giá trung bình cao nhất (VND)'].apply(lambda x: f"{x:,.0f}")
print("\nTháng có giá vé trung bình cao nhất theo từng hãng:\n")
print(result)




# Tính trung bình giá vé theo hãng bay
avg_price_by_airline = df_all.groupby("operating_airline_name")["price_vnd"].mean().sort_values()

# Hiển thị hãng có giá vé trung bình rẻ nhất
print("Hãng có giá vé trung bình rẻ nhất:")
print(avg_price_by_airline.head(1).apply(lambda x: f"{x:,.0f} VND"))

# Hiển thị toàn bộ để so sánh
print("\nGiá vé trung bình theo từng hãng:")
print(avg_price_by_airline.apply(lambda x: f"{x:,.0f} VND"))




avg_prices = df_all.groupby(['booking_class', 'fare_code'])["price_vnd"].mean().sort_values()
print(avg_prices.apply(lambda x: f"{x:,.0f} VND"))



# === Data Exploration ‒ Part 2 (from 02_data_exploration_part2.ipynb) ===

import pandas as pd
import os

# Cài đặt hiển thị đẹp
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.max_colwidth', None)

# Đường dẫn thư mục chứa file
RAW_PATH = "E:\\DATA-PROCESSING\\Du_an_tot_nghiep\\raw_data\\raw_data"

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

# 1. Vietjet: xoá airline & is_duplicate
df_vietjet = load_and_prepare(["vietjet_1.csv", "vietjet_2.csv"],
                              drop_airline=True, drop_is_duplicate=True)

# 2. Vietnam Airlines: xoá airline
df_vnairlines = load_and_prepare(["VNairlines_1_1.csv", "VNairlines_2_2.csv"],
                                 drop_airline=True)

# 3. Bamboo: giữ nguyên hoàn toàn
df_bamboo = load_and_prepare(["BambooAirway.csv"])

# Gộp tất cả
df_all = pd.concat([df_vietjet, df_vnairlines, df_bamboo], ignore_index=True)

# Xem kết quả
print("Tổng số dòng sau khi gộp:", len(df_all))
print(df_all.head(10))



# Kiểm tra có bao nhiêu hãng bay 
unique_airlines = df_all['operating_airline_name'].dropna().unique()
print("Số lượng hãng bay duy nhất trong 'operating_airline_name':", len(unique_airlines))
print("🛫 Danh sách các hãng bay:")
print(unique_airlines)




df_all.drop(columns=['source.name', 'departure_time', 'arrival_time', 'status'], inplace=True)
print("Đã xóa các cột: source.name, departure_time, arrival_time, status khỏi df_all.")





print("🧾 Các cột hiện có trong df_all:")
print(df_all.columns.tolist())




df_all.info()



# Ép kiểu cột date về datetime
df_all['date'] = pd.to_datetime(df_all['date'], errors='coerce')



# Tạo cột giờ
df_all['hour'] = df_all['date'].dt.hour

# Tạo cột "khoảng thời gian bay"
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



# Đảm bảo date đã đúng kiểu datetime
df_all['date'] = pd.to_datetime(df_all['date'], errors='coerce')

# Thêm các cột thời gian
df_all['month'] = df_all['date'].dt.month
df_all['weekday'] = df_all['date'].dt.day_name()
df_all['week'] = df_all['date'].dt.isocalendar().week



df_all.info()
df_all



# Số chuyến bay tổng cộng
print("Tổng số chuyến bay:", len(df_all))

# Số chuyến theo hãng
print("\nSố chuyến theo hãng:")
print(df_all['operating_airline_name'].value_counts())

# Thống kê giá vé
print("\nThống kê giá vé (price_vnd):")
print(df_all['price_vnd'].describe())



print("\nGiá vé trung bình theo tháng:")
print(df_all.groupby('month')['price_vnd'].mean().round(0))



print("\nGiá vé trung bình theo ngày trong tuần:")
print(df_all.groupby('weekday')['price_vnd'].mean().round(0))



print("\nGiá vé trung bình theo tuần:")
print(df_all.groupby('week')['price_vnd'].mean().round(0))




# Giả sử df_all là DataFrame đã được gộp sẵn từ các file
# Kiểm tra các cột có liên quan
print(df_all[['route', 'price_vnd', 'cabin', 'operating_airline_name', 'booking_class', 'fare_code', 'fare_conditions']].head())

# 1. So sánh chênh lệch giá theo hạng vé (Economy vs Business)
cabin_avg_price = df_all.groupby('cabin')['price_vnd'].mean().sort_values()
print("\n[1] Giá trung bình theo Cabin:\n", cabin_avg_price.apply(lambda x: f"{x:,.0f} VND"))

# 2. So sánh giá Economy/Business giữa các hãng cùng tuyến
cabin_airline_route = df_all.groupby(['route', 'cabin', 'operating_airline_name'])['price_vnd'].mean().reset_index()
pivot_price_compare = cabin_airline_route.pivot_table(index=['route', 'cabin'], 
                                                       columns='operating_airline_name', 
                                                       values='price_vnd')
print("\n[2] So sánh giá giữa các hãng trên cùng tuyến (theo Cabin):\n", pivot_price_compare.applymap(lambda x: f"{x:,.0f} VND" if pd.notnull(x) else "-"))







# 3. Xem hãng nào giá tốt hơn cho từng hạng
def better_price(row):
    values = row.dropna()
    if len(values) >= 2:
        return values.idxmin()
    return None

pivot_price_compare['Best_price_airline'] = pivot_price_compare.apply(better_price, axis=1)
print("\n[3] Hãng có giá tốt nhất theo từng tuyến và hạng:\n", pivot_price_compare[['Best_price_airline']])

# 4. Phân tích độ linh hoạt (Booking_Class, Fare_Code, Fare_Conditions)
flex_analysis = df_all.groupby(['booking_class', 'fare_code', 'fare_conditions'])['price_vnd'].agg(['mean', 'count']).sort_values(by='mean')
print("\n[4] Phân tích độ linh hoạt:\n", flex_analysis.head(10))  # hiển thị 10 dòng đầu



# Đảm bảo cột ngày là dạng datetime
df_all['date'] = pd.to_datetime(df_all['date'])

# Đếm số chuyến bay theo từng ngày
flights_per_day = df_all['date'].value_counts().sort_values(ascending=False)

# Hiển thị top 10 ngày có nhiều chuyến nhất
print("Top 10 ngày có nhiều chuyến bay nhất:")
print(flights_per_day.head(10))




# Trung bình giá vé theo tháng
month_avg_price = df_all.groupby('month')['price_vnd'].mean().sort_values(ascending=False)

print("\n[3] Giá vé trung bình theo tháng (giảm dần):\n")
print(month_avg_price.apply(lambda x: f"{x:,.0f} VND"))

# Tháng có giá vé trung bình cao nhất
max_month = month_avg_price.idxmax()
max_price = month_avg_price.max()

print(f"\nTháng có giá vé trung bình cao nhất: Tháng {max_month} với mức giá trung bình: {max_price:,.0f} VND")




# Trung bình giá vé theo tháng và hãng
monthly_airline_price = df_all.groupby(['operating_airline_name', 'month'])['price_vnd'].mean()

# Tìm tháng có giá cao nhất cho mỗi hãng
highest_month_per_airline = monthly_airline_price.groupby('operating_airline_name').idxmax()
highest_price_per_airline = monthly_airline_price.groupby('operating_airline_name').max()

# Gộp kết quả
result = pd.DataFrame({
    'Tháng cao nhất': highest_month_per_airline.map(lambda x: x[1]),
    'Giá trung bình cao nhất (VND)': highest_price_per_airline
})
result['Giá trung bình cao nhất (VND)'] = result['Giá trung bình cao nhất (VND)'].apply(lambda x: f"{x:,.0f}")
print("\nTháng có giá vé trung bình cao nhất theo từng hãng:\n")
print(result)




# Tính trung bình giá vé theo hãng bay
avg_price_by_airline = df_all.groupby("operating_airline_name")["price_vnd"].mean().sort_values()

# Hiển thị hãng có giá vé trung bình rẻ nhất
print("Hãng có giá vé trung bình rẻ nhất:")
print(avg_price_by_airline.head(1).apply(lambda x: f"{x:,.0f} VND"))

# Hiển thị toàn bộ để so sánh
print("\nGiá vé trung bình theo từng hãng:")
print(avg_price_by_airline.apply(lambda x: f"{x:,.0f} VND"))




avg_prices = df_all.groupby(['booking_class', 'fare_code'])["price_vnd"].mean().sort_values()
print(avg_prices.apply(lambda x: f"{x:,.0f} VND"))



#%%
# tổng các chuyến bay theo HÀNH TRÌNH
print(df_all['route'].unique())

route_counts = df_all['route'].value_counts().reset_index()
route_counts.columns = ['route', 'flight_count']

print("Tổng các chuyến bay theo HÀNH TRÌNH:")

route_counts['flight_count'] = route_counts['flight_count'].astype(str) + ' chuyến'

print(route_counts[['route', 'flight_count']])
print(f"Tuyến nhiều lượt bay nhất: {route_counts['flight_count'].max()}")
print(f"Tuyến ít lượt bay nhất: {route_counts['flight_count'].min()}")



route_price_avg = df_all.groupby('route')['price_vnd'].mean().reset_index()

min_price_row = route_price_avg.loc[route_price_avg['price_vnd'].idxmin()]

max_price_row = route_price_avg.loc[route_price_avg['price_vnd'].idxmax()]

print(f"Tuyến bay có giá vé trung bình rẻ nhất: {min_price_row['route']} với giá {min_price_row['price_vnd']:.2f} VND")
print(f"Tuyến bay có giá vé trung bình cao nhất: {max_price_row['route']} với giá {max_price_row['price_vnd']:.2f} VND")



#%%
# Tính giá vé trung bình theo từng tuyến
route_price_avg = df_all.groupby('route')['price_vnd'].mean().reset_index()

# Sắp xếp giá vé trung bình giảm dần
top_highest = route_price_avg.sort_values(by='price_vnd', ascending=False).head(5)

# Sắp xếp giá vé trung bình tăng dần
top_lowest = route_price_avg.sort_values(by='price_vnd', ascending=True).head(5)

print("Top 5 tuyến bay có giá vé trung bình cao nhất:")
print(top_highest)

print("\nTop 5 tuyến bay có giá vé trung bình thấp nhất:")
print(top_lowest)



#%%

df_all['date'] = pd.to_datetime(df_all['date'], errors='coerce')

# Danh sách ngày lễ lớn Việt Nam
holidays = [
    '2025-01-01',  # Tết Dương lịch
    '2025-02-10',  # 29 Tết
    '2025-02-11',  # 30 Tết
    '2025-02-12',  # Mùng 1
    '2025-02-13',  # Mùng 2
    '2025-02-14',  # Mùng 3
    '2025-02-15',  # Mùng 4
    '2025-02-16',  # Mùng 5
    '2025-04-30',  # Giải phóng miền Nam
    '2025-05-01',  # Quốc tế Lao động
    '2025-09-02'   # Quốc khánh
]

holidays = pd.to_datetime(holidays)

df_all['tag_ngay'] = df_all['date'].apply(lambda x: 'Ngày lễ' if x in holidays else 'Ngày bình thường')

le_df = df_all[df_all['tag_ngay'] == 'Ngày lễ']
print(le_df[['date', 'tag_ngay']])




#%%
# Mô tả sân bay
airport_dict = {
    'DAD': 'Sân bay quốc tế Đà Nẵng',
    'HAN': 'Sân bay quốc tế Nội Bài',
    'HPH': 'Sân bay quốc tế Cát Bi',
    'SGN': 'Sân bay quốc tế Tân Sơn Nhất',
    'CXR': 'Sân bay quốc tế Cam Ranh',
    'DLI': 'Sân bay Liên Khương',
    'HUI': 'Sân bay Phú Bài',
    'PQC': 'Sân bay quốc tế Phú Quốc',
    'UIH': 'Sân bay Phù Cát'
}

# Hàm chuyển đổi route thành tên đầy đủ
def replace_route_with_names(route):
    try:
        dep, arr = route.split('_')
        dep_name = airport_dict.get(dep, dep)
        arr_name = airport_dict.get(arr, arr)
        return f"{dep_name} - {arr_name}"
    except:
        return route  # Giữ nguyên nếu có lỗi

df_all['route_full_name'] = df_all['route'].apply(replace_route_with_names)

# Kiểm tra kết quả
print(df_all[['route', 'route_full_name']].head(10))

df_all.rename(columns={'route_full_name': 'description'}, inplace=True)



# Tính giá gốc (net_price) và tỷ lệ thuế trên tổng giá
df_all['net_price'] = df_all['price_vnd'] - df_all['taxes']
df_all['tax_ratio'] = df_all['taxes'] / df_all['price_vnd']
display(df_all[['price_vnd', 'taxes', 'net_price', 'tax_ratio']].sample(5))




#Phân loại vé thành Economy / Premium / Business
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
display(df_all[['fare_code', 'fare_conditions', 'fare_type']].sample(10))





#Tuyến có mức thuế trung bình cao nhất
avg_tax_by_route = df_all.groupby('route')['taxes'].mean().reset_index().sort_values(by='taxes', ascending=False)
print ("Tuyến có mức thuế trung bình cao nhất: \n")
print(avg_tax_by_route)



##Tỷ lệ từng fare_type trong mỗi hãng
fare_type_ratio = df_all.groupby(['operating_airline_name', 'fare_type'])['price_vnd'].count().reset_index()
fare_type_ratio = fare_type_ratio.pivot(index='operating_airline_name', columns='fare_type', values='price_vnd').fillna(0)
fare_type_ratio = fare_type_ratio.div(fare_type_ratio.sum(axis=1), axis=0)
print('Tỷ lệ từng loại vé mỗi hãng: \n')
fare_type_ratio



#Giá cao nhất theo loại vé của từng hãng
max_price_by_faretype = df_all.groupby(['operating_airline_name', 'fare_type'])['price_vnd'].max().reset_index()
max_price_by_faretype



price_std = df_all.groupby('route')['price_vnd'].std()
df_all['price_std'] = df_all['route'].map(price_std)

# Gán nhãn mức biến động (cao / trung bình / thấp)
df_all['price_volatility'] = pd.qcut(df_all['price_std'], q=3, labels=['Ổn định', 'Trung bình', 'Biến động cao'])

display(df_all[['route', 'price_vnd', 'price_std', 'price_volatility']].sample(5))




# === Feature Filtering & Selection (from 03_feature_filtering_full.ipynb) ===

import pandas as pd
import os

# Cài đặt hiển thị đẹp
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.max_colwidth', None)

# Đường dẫn thư mục chứa file
RAW_PATH = "/Users/nthanhdat/Documents/Fpt_Polytechnic/Graduation_Project/data_pipeline/raw_data"

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

# 1. Vietjet: xoá airline & is_duplicate
df_vietjet = load_and_prepare(["vietjet_1.csv", "vietjet_2.csv"],
                              drop_airline=True, drop_is_duplicate=True)

# 2. Vietnam Airlines: xoá airline
df_vnairlines = load_and_prepare(["VNairlines_1_1.csv", "VNairlines_2_2.csv"],
                                 drop_airline=True)

# 3. Bamboo: giữ nguyên hoàn toàn
df_bamboo = load_and_prepare(["BambooAirway.csv"])

# Gộp tất cả
df_all = pd.concat([df_vietjet, df_vnairlines, df_bamboo], ignore_index=True)

# Xem kết quả
print("Tổng số dòng sau khi gộp:", len(df_all))
print(df_all.head(10))




# Status bỏ vì chứa soure file thôi không có thông tin gì khác
# Cột flight_id giữ
flight_id_counts = df_all['flight_id'].value_counts()
print(flight_id_counts.head(10))  # Xem top 10 chuyến bị trùng

df_dupes = df_all[df_all['flight_id'] == "SEG-VN1341-CXRSGN-2025-06-14-0840"]

print("✅ Số bản ghi:", len(df_dupes))
print("✅ Số mức giá khác nhau:", df_dupes['price_vnd'].nunique())
print("🪙 Các mức giá:")
print(df_dupes['price_vnd'].unique())







#### Cột departure_time arrival_time bỏ
# Kiểm tra tổng số dòng
total_rows = len(df_all)

# Đếm số lượng NaN trong từng cột
departure_missing = df_all['departure_time'].isna().sum()
arrival_missing = df_all['arrival_time'].isna().sum()

# In kết quả
print(f"📌 Tổng số dòng: {total_rows}")
print(f"🛫 'departure_time' bị thiếu: {departure_missing} dòng ({departure_missing / total_rows:.2%})")
print(f"🛬 'arrival_time' bị thiếu:   {arrival_missing} dòng ({arrival_missing / total_rows:.2%})")

has_departure_data = df_all['departure_time'].notna().any()
has_arrival_data = df_all['arrival_time'].notna().any()

print("✅ Có dữ liệu departure_time không?:", has_departure_data)
print("✅ Có dữ liệu arrival_time không?:", has_arrival_data)

 



# Đoạn code kiểm tra 5 cột: quota, status, service_code, fare_conditions, operating_airline_code có nên xóa không ?
# Kết quả status là xóa 
columns_to_check = ['quota', 'status', 'service_code', 'fare_conditions', 'operating_airline_code']

total_rows = len(df_all)

for col in columns_to_check:
    print(f"\n🔍 Cột: {col}")
    if col not in df_all.columns:
        print("❗ Cột không tồn tại trong dữ liệu.")
        continue

    unique_vals = df_all[col].dropna().unique()
    unique_count = len(unique_vals)
    null_count = df_all[col].isna().sum()
    null_percent = null_count / total_rows * 100

    print(f"• Số giá trị khác nhau (unique): {unique_count}")
    print(f"• Số dòng bị thiếu (NaN): {null_count} / {total_rows} ({null_percent:.2f}%)")
    print(f"• Một vài giá trị đầu tiên: {unique_vals[:5]}")

    # Gợi ý giữ/xoá đơn giản
    if unique_count == 1 and null_percent < 10:
        print("👉 Gợi ý: ❌ Có thể XÓA (chỉ có 1 giá trị)")
    elif null_percent > 95:
        print("👉 Gợi ý: ❌ Có thể XÓA (quá nhiều dữ liệu thiếu)")
    else:
        print("👉 Gợi ý: ✅ CÂN NHẮC GIỮ (có thông tin phân biệt)")





# === Save processed dataset ===
try:
    processed_df.to_parquet("data/processed_data.parquet", index=False)
    print("✅ Processed data saved to data/processed_data.parquet")
except NameError:
    print("⚠️  Variable 'processed_df' not found. Ensure your last step assigns the final DataFrame to 'processed_df'.")

# === Processing Functions (from 04_processing_functions.py) ===
#%%
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os

file_path = r'D:\Fpt\Dự án tốt nghiệp\Source\raw_data'
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.max_colwidth', None)


def load_and_prepare(files, drop_airline=False, drop_is_duplicate=False):
    dfs = []
    for file in files:
        df = pd.read_csv(os.path.join(file_path, file))
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

        if drop_airline and "airline" in df.columns:
            df.drop(columns="airline", inplace=True)
        if drop_is_duplicate and "is_duplicate" in df.columns:
            df.drop(columns="is_duplicate", inplace=True)

        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


# 1. Vietjet: xoá airline & is_duplicate
df_vietjet = load_and_prepare(["vietjet_1.csv", "vietjet_2.csv"],
                              drop_airline=True, drop_is_duplicate=True)

# 2. Vietnam Airlines: xoá airline
df_vnairlines = load_and_prepare(["VNairlines_1_1.csv", "VNairlines_2_2.csv"],
                                 drop_airline=True)

# 3. Bamboo: giữ nguyên hoàn toàn
df_bamboo = load_and_prepare(["BambooAirway.csv"])

# Gộp tất cả
df_all = pd.concat([df_vietjet, df_vnairlines, df_bamboo], ignore_index=True)

# Xem kết quả
print("Tổng số dòng sau khi gộp:", len(df_all))
print(df_all.head(10))

#%%
# tổng các chuyến bay theo HÀNH TRÌNH
print(df_all['route'].unique())

route_counts = df_all['route'].value_counts().reset_index()
route_counts.columns = ['route', 'flight_count']

print("Tổng các chuyến bay theo HÀNH TRÌNH:")

route_counts['flight_count'] = route_counts['flight_count'].astype(str) + ' chuyến'

print(route_counts[['route', 'flight_count']])
print(f"Tuyến nhiều lượt bay nhất: {route_counts['flight_count'].max()}")
print(f"Tuyến ít lượt bay nhất: {route_counts['flight_count'].min()}")
#%%
route_price_avg = df_all.groupby('route')['price_vnd'].mean().reset_index()

min_price_row = route_price_avg.loc[route_price_avg['price_vnd'].idxmin()]

max_price_row = route_price_avg.loc[route_price_avg['price_vnd'].idxmax()]

print(f"Tuyến bay có giá vé trung bình rẻ nhất: {min_price_row['route']} với giá {min_price_row['price_vnd']:.2f} VND")
print(f"Tuyến bay có giá vé trung bình cao nhất: {max_price_row['route']} với giá {max_price_row['price_vnd']:.2f} VND")


#%%
# Tính giá vé trung bình theo từng tuyến
route_price_avg = df_all.groupby('route')['price_vnd'].mean().reset_index()

# Sắp xếp giá vé trung bình giảm dần
top_highest = route_price_avg.sort_values(by='price_vnd', ascending=False).head(5)

# Sắp xếp giá vé trung bình tăng dần
top_lowest = route_price_avg.sort_values(by='price_vnd', ascending=True).head(5)

print("Top 5 tuyến bay có giá vé trung bình cao nhất:")
print(top_highest)

print("\nTop 5 tuyến bay có giá vé trung bình thấp nhất:")
print(top_lowest)
#%%

df_all['date'] = pd.to_datetime(df_all['date'], errors='coerce')

# Danh sách ngày lễ lớn Việt Nam
holidays = [
    '2025-01-01',  # Tết Dương lịch
    '2025-02-10',  # 29 Tết
    '2025-02-11',  # 30 Tết
    '2025-02-12',  # Mùng 1
    '2025-02-13',  # Mùng 2
    '2025-02-14',  # Mùng 3
    '2025-02-15',  # Mùng 4
    '2025-02-16',  # Mùng 5
    '2025-04-30',  # Giải phóng miền Nam
    '2025-05-01',  # Quốc tế Lao động
    '2025-09-02'   # Quốc khánh
]

holidays = pd.to_datetime(holidays)

df_all['tag_ngay'] = df_all['date'].apply(lambda x: 'Ngày lễ' if x in holidays else 'Ngày bình thường')

le_df = df_all[df_all['tag_ngay'] == 'Ngày lễ']
print(le_df[['date', 'tag_ngay']])
#%%
# Mô tả sân bay
airport_dict = {
    'DAD': 'Sân bay quốc tế Đà Nẵng',
    'HAN': 'Sân bay quốc tế Nội Bài',
    'HPH': 'Sân bay quốc tế Cát Bi',
    'SGN': 'Sân bay quốc tế Tân Sơn Nhất',
    'CXR': 'Sân bay quốc tế Cam Ranh',
    'DLI': 'Sân bay Liên Khương',
    'HUI': 'Sân bay Phú Bài',
    'PQC': 'Sân bay quốc tế Phú Quốc',
    'UIH': 'Sân bay Phù Cát'
}

# Hàm chuyển đổi route thành tên đầy đủ
def replace_route_with_names(route):
    try:
        dep, arr = route.split('_')
        dep_name = airport_dict.get(dep, dep)
        arr_name = airport_dict.get(arr, arr)
        return f"{dep_name} - {arr_name}"
    except:
        return route  # Giữ nguyên nếu có lỗi

df_all['route_full_name'] = df_all['route'].apply(replace_route_with_names)

# Kiểm tra kết quả
print(df_all[['route', 'route_full_name']].head(10))

df_all.rename(columns={'route_full_name': 'description'}, inplace=True)
