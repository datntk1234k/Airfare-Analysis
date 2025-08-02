-- giá vé trung bình mỗi tháng
SELECT 
    a.airline_name,
    AVG(p.price_vnd) AS avg_price
FROM Fact_Prices p
JOIN Dim_Flights f ON p.flight_id = f.flight_id
JOIN Dim_Airlines a ON f.airline_id = a.airline_id
GROUP BY a.airline_name
ORDER BY avg_price;

-- Liệt kê tất cả các chuyến bay trong tháng 8/2025
SELECT *
FROM Dim_Flights
WHERE MONTH(flight_date) = 8 AND YEAR(flight_date) = 2025;

-- Danh sách giá vé rẻ hơn 1 triệu đồng
SELECT *
FROM Fact_Prices
WHERE price_vnd < 1000000;

--  Tìm các chuyến bay của hãng "VietJet"
SELECT f.*
FROM Dim_Flights f
JOIN Dim_Airlines a ON f.airline_id = a.airline_id
WHERE a.airline_name = 'VietJet';