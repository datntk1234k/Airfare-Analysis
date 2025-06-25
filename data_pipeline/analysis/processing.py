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
