-- View xem thông tin giá bay, hãng, sân bay đi/đến
CREATE VIEW vw_FlightPrices AS
SELECT 
    f.flight_id,
    f.flight_date,
    f.flight_code,
    a.airline_name,
    ao.city AS origin,
    ad.city AS destination,
    fc.booking_class,
    fc.fare_code,
    fc.cabin,
    fs.service_code,
    fs.fare_conditions,
    p.price_vnd
FROM Fact_Prices p
JOIN Dim_Flights f ON p.flight_id = f.flight_id
JOIN Dim_Airlines a ON f.airline_id = a.airline_id
JOIN Dim_Airport ao ON f.origin_airport_id = ao.airport_id
JOIN Dim_Airport ad ON f.dest_airport_id = ad.airport_id
JOIN Dim_flight_service fs ON p.flight_id = fs.flight_id AND p.fare_class_id = fs.fare_class_id
JOIN Dim_Fare_class fc ON fs.fare_class_id = fc.fare_class_id;
SELECT * FROM vw_FlightPrices WHERE origin = 'Hanoi'