INSERT INTO Dim_Airport (city)
SELECT DISTINCT
    CASE 
        WHEN origin = 'SGN' THEN 'Ho Chi Minh City'
        WHEN origin = 'HAN' THEN 'Hanoi'
        WHEN origin = 'DAD' THEN 'Da Nang'
        WHEN origin = 'CXR' THEN 'Nha Trang'
        WHEN origin = 'HUI' THEN 'Hue'
        WHEN origin = 'DLI' THEN 'Da Lat'
        WHEN origin = 'PQC' THEN 'Phu Quoc'
        WHEN origin = 'VII' THEN 'Vinh'
        WHEN origin = 'VCA' THEN 'Can Tho'
        WHEN origin = 'BMV' THEN 'Buon Ma Thuot'
        WHEN origin = 'THD' THEN 'Thanh Hoa'
        WHEN origin = 'VDO' THEN 'Van Don'
        WHEN origin = 'VDH' THEN 'Dong Hoi'
        WHEN origin = 'TBB' THEN 'Tuy Hoa'
        WHEN origin = 'PXU' THEN 'Pleiku'
        WHEN origin = 'UIH' THEN 'Quy Nhon'
        WHEN origin = 'VCL' THEN 'Chu Lai'
        WHEN origin = 'DIN' THEN 'Dien Bien'
        WHEN origin = 'CAH' THEN 'Ca Mau'
        WHEN origin = 'VKG' THEN 'Rach Gia'
        WHEN origin = 'HPH' THEN 'Hai Phong'
        WHEN origin = 'VCS' THEN 'Con Dao'
        WHEN origin = 'CJM' THEN 'Con Dao (Co Ong)'
        WHEN origin = 'BMQ' THEN 'Phan Thiet'
        WHEN origin = 'CRX' THEN 'Con Dao (Côn Đảo - alternate code)'
        WHEN origin = 'GL' THEN 'Gia Lai (Pleiku)'
        WHEN origin = 'NT' THEN 'Nha Trang (alternate code)'
        WHEN origin = 'HGN' THEN 'Ha Giang (proposed)'
        WHEN origin = 'YBM' THEN 'Yen Bai (proposed)'
        ELSE 'Unknown'
    END AS city
FROM [dbo].[airline_cleaned_clean2]
WHERE origin IS NOT NULL;

-- chèn dữ liệu cho Dim_airlines
INSERT INTO Dim_Airlines (airline_name, operating_airline_code)
SELECT DISTINCT
    operating_airline_name,
    operating_airline_code
FROM [dbo].[airline_cleaned_clean2]
WHERE operating_airline_name IS NOT NULL AND operating_airline_code IS NOT NULL;

--chèn dữ liệu vào Dim_Fare_Class
INSERT INTO Dim_Fare_class (fare_code, cabin, booking_class)
SELECT DISTINCT
    fare_code,
    cabin,
    booking_class
FROM [dbo].[airline_cleaned_clean2]
WHERE fare_code IS NOT NULL;

--chèn dữ liệu vào Dim_Date
INSERT INTO Dim_Date (calendar_date, year, month, day, dow, is_weekend)
SELECT DISTINCT
    CONVERT(DATE, [date]) AS calendar_date,
    year,
    month,
    day,
    dow,
    CASE 
        WHEN dow IN (6, 7) THEN 1 
        ELSE 0 
    END AS is_weekend
FROM [dbo].[airline_cleaned_clean2]
WHERE [date] IS NOT NULL;

--chèn dữ liệu vào Dim_Flight

-- Insert into Dim_Flights
INSERT INTO Dim_Flights ( flight_date, flight_code, flight_number, airline_id, origin_airport_id, dest_airport_id)
SELECT DISTINCT 
    c.flight_date,
    c.flight_code,
    c.flight_number,
    da.airline_id,
    ao.airport_id AS origin_airport_id,
    ad.airport_id AS dest_airport_id
FROM [dbo].[airline_cleaned_clean2] c
LEFT JOIN Dim_Airlines da
    ON c.operating_airline_code = da.operating_airline_code
LEFT JOIN Dim_Airport ao
    ON CASE c.origin
        WHEN 'SGN' THEN 'Ho Chi Minh City'
        WHEN 'HAN' THEN 'Hanoi'
        WHEN 'DAD' THEN 'Da Nang'
        WHEN 'CXR' THEN 'Nha Trang'
        WHEN 'HUI' THEN 'Hue' 
        WHEN 'DLI' THEN 'Da Lat' 
        WHEN 'PQC' THEN 'Phu Quoc' 
        WHEN 'VII' THEN 'Vinh'
        WHEN 'VCA' THEN 'Can Tho'
        WHEN 'BMV' THEN 'Buon Ma Thuot'
        WHEN 'THD' THEN 'Thanh Hoa'
        WHEN 'VDO' THEN 'Van Don'
        WHEN 'VDH' THEN 'Dong Hoi'
        WHEN 'TBB' THEN 'Tuy Hoa'
        WHEN 'PXU' THEN 'Pleiku'
        WHEN 'UIH' THEN 'Quy Nhon'
        WHEN 'VCL' THEN 'Chu Lai'
        WHEN 'DIN' THEN 'Dien Bien'
        WHEN 'CAH' THEN 'Ca Mau'
        WHEN 'VKG' THEN 'Rach Gia'
        WHEN 'HPH' THEN 'Hai Phong'
        WHEN 'VCS' THEN 'Con Dao'
        WHEN 'CJM' THEN 'Con Dao (Co Ong)'
        WHEN 'BMQ' THEN 'Phan Thiet'
        WHEN 'CRX' THEN 'Con Dao (Côn Đảo - alternate code)'
        WHEN 'GL' THEN 'Gia Lai (Pleiku)'
        WHEN 'NT' THEN 'Nha Trang (alternate code)'
        WHEN 'HGN' THEN 'Ha Giang (proposed)'
        WHEN 'YBM' THEN 'Yen Bai (proposed)'
        ELSE 'Unknown'
    END = ao.city
LEFT JOIN Dim_Airport ad
    ON CASE c.destination
        WHEN 'SGN' THEN 'Ho Chi Minh City'
        WHEN 'HAN' THEN 'Hanoi'
        WHEN 'DAD' THEN 'Da Nang'
        WHEN 'CXR' THEN 'Nha Trang'
        WHEN 'HUI' THEN 'Hue' 
        WHEN 'DLI' THEN 'Da Lat' 
        WHEN 'PQC' THEN 'Phu Quoc' 
        WHEN 'VII' THEN 'Vinh'
        WHEN 'VCA' THEN 'Can Tho'
        WHEN 'BMV' THEN 'Buon Ma Thuot'
        WHEN 'THD' THEN 'Thanh Hoa'
        WHEN 'VDO' THEN 'Van Don'
        WHEN 'VDH' THEN 'Dong Hoi'
        WHEN 'TBB' THEN 'Tuy Hoa'
        WHEN 'PXU' THEN 'Pleiku'
        WHEN 'UIH' THEN 'Quy Nhon'
        WHEN 'VCL' THEN 'Chu Lai'
        WHEN 'DIN' THEN 'Dien Bien'
        WHEN 'CAH' THEN 'Ca Mau'
        WHEN 'VKG' THEN 'Rach Gia'
        WHEN 'HPH' THEN 'Hai Phong'
        WHEN 'VCS' THEN 'Con Dao'
        WHEN 'CJM' THEN 'Con Dao (Co Ong)'
        WHEN 'BMQ' THEN 'Phan Thiet'
        WHEN 'CRX' THEN 'Con Dao (Côn Đảo - alternate code)'
        WHEN 'GL' THEN 'Gia Lai (Pleiku)'
        WHEN 'NT' THEN 'Nha Trang (alternate code)'
        WHEN 'HGN' THEN 'Ha Giang (proposed)'
        WHEN 'YBM' THEN 'Yen Bai (proposed)'
        ELSE 'Unknown'
    END = ad.city;


 -- chèn dữ liệu cho Dim_flight_service 
INSERT INTO Dim_flight_service (flight_id, fare_class_id, service_code, fare_conditions)
SELECT
    f.flight_id,
    fc.fare_class_id,
    ac.service_code,
    ac.fare_conditions
FROM [dbo].[airline_cleaned_clean2] ac
JOIN Dim_Flights f 
    ON ac.flight_code = f.flight_code AND ac.flight_date = f.flight_date
JOIN Dim_Fare_class fc 
    ON ac.fare_code = fc.fare_code AND ac.booking_class = fc.booking_class AND ac.cabin = fc.cabin;
