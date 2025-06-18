#%%
import pandas as pd
import numpy as np
import math

# Đọc file CSV
file_path = r'D:\Fpt\Dự án tốt nghiệp\Source\vietjet_data_2.csv'
df = pd.read_csv(file_path)

# Chuyển cột Date sang datetime
df['Date'] = pd.to_datetime(df['Date'], format="%m/%d/%Y")

# Lọc ra các dòng có ngày là Chủ nhật
sunday_df = df[df['Date'].dt.weekday == 6]

# Tạo danh sách để chứa dữ liệu nhân bản
duplicated_rows = []

# Lặp qua từng ngày Chủ nhật riêng biệt
for sunday_date, group in sunday_df.groupby('Date'):
    n_rows = len(group)

    # Tính số bản sao cần thêm cho ngày này (6–8%)
    percent_increase = np.random.uniform(0.06, 0.08)
    n_duplicates = math.ceil(n_rows * percent_increase)

    if n_duplicates > 0:
        duplicated_sample = group.sample(n=n_duplicates, replace=True).copy()
        duplicated_rows.append(duplicated_sample)

# Gộp tất cả dữ liệu nhân bản lại
if duplicated_rows:
    additional_data = pd.concat(duplicated_rows, ignore_index=True)
    additional_data['Is_Duplicate'] = True
else:
    additional_data = pd.DataFrame(columns=df.columns)
    additional_data['Is_Duplicate'] = []

# Thêm cờ đánh dấu bản gốc
df['Is_Duplicate'] = False

# Gộp dữ liệu gốc và dữ liệu nhân bản
df_augmented = pd.concat([df, additional_data], ignore_index=True)

# Xuất kết quả ra file
output_path = r"D:\Fpt\Dự án tốt nghiệp\Source\vietjet_augmented_sundays_2.csv"
df_augmented.to_csv(output_path, index=False)

# In thông tin
print(f"Số ngày Chủ nhật: {sunday_df['Date'].nunique()}")
print(f"Tổng chuyến bay gốc ngày Chủ nhật: {len(sunday_df)}")
print(f"Tổng chuyến bay được thêm: {len(additional_data)}")
print(f"Tổng số dòng sau khi thêm: {len(df_augmented)}")


#%%
import pandas as pd

# Đọc dữ liệu
file_path = r'D:\Fpt\Dự án tốt nghiệp\Source\vietjet_data.csv'
df = pd.read_csv(file_path)

# Chuyển cột Date sang datetime
df['Date'] = pd.to_datetime(df['Date'])


# Chọn ngày cụ thể: 15-06-2025
target_date = pd.to_datetime("29/06/2025")

# Lọc các dòng có ngày trùng với 15-06-2025
flights_on_target_date = df[df['Date'].dt.date == target_date.date()]

# In số lượng chuyến bay và vài dòng ví dụ
print(f"Tổng số chuyến bay vào ngày {target_date.date()}: {len(flights_on_target_date)}")
print(flights_on_target_date.head())
#%%
import pandas as pd

# Đọc dữ liệu
file_path = r'D:\Fpt\Dự án tốt nghiệp\Source\vietjet_augmented_sundays.csv'
df = pd.read_csv(file_path)

# Chuyển cột Date sang datetime
df['Date'] = pd.to_datetime(df['Date'])


# Chọn ngày cụ thể: 15-06-2025
target_date = pd.to_datetime("29/06/2025")

# Lọc các dòng có ngày trùng với 15-06-2025
flights_on_target_date = df[df['Date'].dt.date == target_date.date()]

# In số lượng chuyến bay và vài dòng ví dụ
print(f"Tổng số chuyến bay vào ngày {target_date.date()}: {len(flights_on_target_date)}")
print(flights_on_target_date.head())
