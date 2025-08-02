-- tạo proc tìm giá vé thấp nhất cho 1 cặp sân bay, trong 1 khoảng thời gian
CREATE PROCEDURE sp_GetLowestFare
    @origin VARCHAR(100),
    @destination VARCHAR(100),
    @startDate DATE,
    @endDate DATE
AS
BEGIN
    SELECT TOP 1 
        f.flight_date, 
        f.flight_code, 
        a.airline_name,
        MIN(p.price_vnd) AS lowest_price
    FROM Fact_Prices p
    JOIN Dim_Flights f ON p.flight_id = f.flight_id
    JOIN Dim_Airlines a ON f.airline_id = a.airline_id
    JOIN Dim_Airport ao ON f.origin_airport_id = ao.airport_id
    JOIN Dim_Airport ad ON f.dest_airport_id = ad.airport_id
    WHERE ao.city = @origin AND ad.city = @destination
        AND f.flight_date BETWEEN @startDate AND @endDate
    GROUP BY f.flight_date, f.flight_code, a.airline_name
    ORDER BY lowest_price ASC;
END;
EXEC sp_GetLowestFare 'Ho Chi Minh City', 'Hanoi', '2025-08-01', '2025-08-10';

-- tính tổng doanh thu theo hãng
CREATE PROCEDURE sp_GetTotalRevenueByAirline
    @airline_name VARCHAR(100)
AS
BEGIN
    SELECT a.airline_name, SUM(fp.price_vnd) AS revenue
    FROM Fact_Prices fp
    JOIN Dim_Flights f ON fp.flight_id = f.flight_id
    JOIN Dim_Airlines a ON f.airline_id = a.airline_id
    WHERE a.airline_name = @airline_name
    GROUP BY a.airline_name;
END;

EXEC sp_GetTotalRevenueByAirline @airline_name = N'Vietnam Airlines';


--xóa dữ liệu vé theo ngày
CREATE PROCEDURE sp_TopExpensiveFlightByDate
    @date DATE
AS
BEGIN
    SELECT TOP 1 f.flight_code, p.price_vnd
    FROM Fact_Prices p
    JOIN Dim_Flights f ON p.flight_id = f.flight_id
    WHERE f.flight_date = @date
    ORDER BY p.price_vnd DESC
END;


-- tạo bảng báo cáo doanh thu theo từng ngày
CREATE PROCEDURE sp_RevenuePerDay
AS
BEGIN
    SELECT d.full_date, SUM(fp.price_vnd) AS total_revenue
    FROM Fact_Prices fp
    JOIN Dim_Date d ON fp.date_id = d.date_id
    GROUP BY d.full_date
    ORDER BY d.full_date
END;

EXEC sp_RevenuePerDay;

-- function xác định ngày cuối tuần 
CREATE FUNCTION fn_IsWeekend (@date DATE)
RETURNS BIT
AS
BEGIN
    DECLARE @dow INT = DATEPART(WEEKDAY, @date)
    RETURN CASE WHEN @dow IN (1,7) THEN 1 ELSE 0 END
END;

SELECT dbo.fn_IsWeekend('2025-08-02') AS IsWeekend;

-- tìm chuyến bay theo thành phố 
CREATE FUNCTION fn_GetFlightsByCity (@city VARCHAR(100))
RETURNS TABLE
AS
RETURN (
    SELECT f.*
    FROM Dim_Flights f
    JOIN Dim_Airport a ON f.origin_airport_id = a.airport_id
    WHERE a.city = @city
);

SELECT * FROM dbo.fn_GetFlightsByCity('Hà Nội');