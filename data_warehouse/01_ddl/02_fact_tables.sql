-- 7. Fact_Prices
-- ============================================
CREATE TABLE Fact_Prices (
    price_id INT PRIMARY KEY IDENTITY(1,1),
    flight_id INT,
    fare_class_id INT,
    date_id INT,
    price_vnd DECIMAL(15,2),
    quota DECIMAL(5,2),
    status VARCHAR(20),
    FOREIGN KEY (flight_id) REFERENCES Dim_Flights(flight_id),
    FOREIGN KEY (fare_class_id) REFERENCES Dim_Fare_class(fare_class_id),
    FOREIGN KEY (date_id) REFERENCES Dim_Date(date_id)
);
