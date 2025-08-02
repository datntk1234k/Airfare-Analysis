-- lọc theo flight code và flight date để tìm giá:
CREATE NONCLUSTERED INDEX idx_FlightCodeDate
ON Dim_Flights (flight_code, flight_date);
