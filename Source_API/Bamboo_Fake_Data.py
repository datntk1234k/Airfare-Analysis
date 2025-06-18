import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Đọc lại file Excel gốc
xls = pd.ExcelFile("E:\\DATA-PROCESSING\\Du_an_tot_nghiep\\Bambooairways\\Bambooairways.xlsx")
df = pd.read_excel(xls, sheet_name="Bamboo")

# Lấy route thật để dùng lại y chang
routes = df['Route'].unique()
fare_classes = df[['Fare_Code', 'Fare_Class', 'Booking_Class', 'Cabin']].drop_duplicates()
avg_price_tax = df.groupby('Fare_Class')[['Price_VND', 'Taxes']].mean().reset_index()

# Mục tiêu: tạo 10,000 dòng dữ liệu giả
target_rows = 10000
fake_data = []

# Bắt đầu từ ngày 01/01/2026
base_date = datetime(2026, 1, 1)
days_span = 180  # khoảng 6 tháng
rows_per_day = max(1, target_rows // days_span)

while len(fake_data) < target_rows:
    for day_offset in range(days_span):
        current_date = base_date + timedelta(days=day_offset)
        for _ in range(rows_per_day):
            fare = fare_classes.sample(1).iloc[0]
            route = np.random.choice(routes)
            origin, destination = route.split('_')

            base = avg_price_tax[avg_price_tax['Fare_Class'] == fare['Fare_Class']]
            if base.empty:
                continue

            price = int(base['Price_VND'].values[0] * np.random.uniform(0.95, 1.10))
            tax = int(base['Taxes'].values[0] * np.random.uniform(0.95, 1.05))

            dep_hour = np.random.randint(5, 22)
            dep_min = np.random.choice([0, 15, 30, 45])
            dep_time = datetime(current_date.year, current_date.month, current_date.day, dep_hour, dep_min)
            arr_time = dep_time + timedelta(hours=2, minutes=np.random.randint(0, 30))

            flight_num = f"QH{np.random.randint(200, 299)}"
            flight_id = f"SEG-{flight_num}-{route}-{current_date.strftime('%Y-%m-%d')}-{dep_hour*100 + dep_min}"

            fake_data.append({
                'Source.Name': 'bamboo_flight_prices.csv',
                'Date': current_date,
                'Route': route,
                'Flight_ID': flight_id,
                'Origin': origin,
                'Destination': destination,
                'Fare_Code': fare['Fare_Code'],
                'Fare_Class': fare['Fare_Class'],
                'Price_VND': price,
                'Taxes': tax,
                'Departure_Time': dep_time,
                'Arrival_Time': arr_time,
                'Booking_Class': fare['Booking_Class'],
                'Cabin': fare['Cabin'],
                'Quota': np.random.randint(2, 10),
                'Status': 'HK',
                'Service_Code': np.nan,
                'Fare_Conditions': 'PEN1,PEN2,PEN3',
                'Operating_Airline_Code': 'QH',
                'Operating_Airline_Name': 'BAMBOO AIRWAYS'
            })

        if len(fake_data) >= target_rows:
            break

# Tạo DataFrame và lưu file
fake_df = pd.DataFrame(fake_data[:target_rows])
fake_df.to_csv("Bambooairways2.csv", index=False, encoding="utf-8-sig")
fake_df.shape
