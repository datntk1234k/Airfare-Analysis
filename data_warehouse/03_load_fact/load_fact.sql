-- chèn dữ liệu cho bảng Fact_Prices
INSERT INTO Fact_Prices (flight_id, fare_class_id, date_id, price_vnd, quota, status)
SELECT
    f.flight_id,
    fc.fare_class_id,
    d.date_id,
    ac.price_vnd,
    ac.quota,
    CASE 
        WHEN ac.is_duplicate = 1 THEN 'Duplicate'
        ELSE 'Valid'
    END AS status
FROM [dbo].[airline_cleaned_clean2] ac 
JOIN Dim_Flights f 
    ON ac.flight_code = f.flight_code AND ac.flight_date = f.flight_date
JOIN Dim_Fare_class fc 
    ON ac.fare_code = fc.fare_code AND ac.booking_class = fc.booking_class AND ac.cabin = fc.cabin
JOIN Dim_Date d 
    ON ac.date = d.calendar_date;




WITH DistinctService AS (
    SELECT Top 1000
        ac.flight_code,
        ac.fare_code,
        ac.cabin,
        ac.booking_class,
        MIN(ac.service_code) AS service_code,
        MIN(ac.fare_conditions) AS fare_conditions 
    FROM dbo.airline_cleaned_clean2 ac 
    WHERE ac.flight_code IS NOT NULL AND ac.fare_code IS NOT NULL 
    GROUP BY ac.flight_code, ac.fare_code, ac.cabin, ac.booking_class 
)
INSERT INTO Dim_flight_service (flight_id, fare_class_id, service_code, fare_conditions)
SELECT 
    f.flight_id,
    fc.fare_class_id,
    ds.service_code,
    ds.fare_conditions
FROM DistinctService ds
JOIN Dim_Flights f ON f.flight_code = ds.flight_code
JOIN Dim_Fare_class fc ON fc.fare_code = ds.fare_code
                      AND fc.cabin = ds.cabin
                      AND fc.booking_class = ds.booking_class;


select * from Dim_flight_service