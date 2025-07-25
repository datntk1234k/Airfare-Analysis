-- ============================================
-- 1. Dim_Airport
-- ============================================
CREATE TABLE Dim_Airport (
    airport_id INT PRIMARY KEY IDENTITY(1,1),
    city VARCHAR(100) NOT NULL
);

-- ============================================
-- 2. Dim_Airlines
-- ============================================
CREATE TABLE Dim_Airlines (
    airline_id INT PRIMARY KEY IDENTITY(1,1),
    airline_name VARCHAR(100) NOT NULL,
    operating_airline_code VARCHAR(10) NOT NULL
);

-- ============================================
-- 3. Dim_Fare_class
-- ============================================
CREATE TABLE Dim_Fare_class (
    fare_class_id INT PRIMARY KEY IDENTITY(1,1),
    fare_code VARCHAR(20) NOT NULL,
    cabin VARCHAR(50),
    booking_class VARCHAR(5)
);

-- ============================================
-- 4. Dim_Date
-- ============================================
CREATE TABLE Dim_Date (
    date_id INT PRIMARY KEY IDENTITY(1,1),
    calendar_date DATE NOT NULL,
    year INT,
    month INT,
    day INT,
    dow INT,
    is_weekend BIT
);

-- ============================================
-- 5. Dim_Flights
-- ============================================
CREATE TABLE Dim_Flights (
    flight_id INT PRIMARY KEY IDENTITY(1,1) ,
    flight_date DATE NOT NULL,
    flight_code VARCHAR(20) NOT NULL,
    flight_number VARCHAR(10),
    airline_id INT,
    origin_airport_id INT,
    dest_airport_id INT,
    FOREIGN KEY (airline_id) REFERENCES Dim_Airlines(airline_id),
    FOREIGN KEY (origin_airport_id) REFERENCES Dim_Airport(airport_id),
    FOREIGN KEY (dest_airport_id) REFERENCES Dim_Airport(airport_id)
);

-- ============================================
-- 6. Dim_flight_service
-- ============================================
CREATE TABLE Dim_flight_service (
    flight_id INT,
    fare_class_id INT,
    service_code VARCHAR(50),
    fare_conditions TEXT,
    PRIMARY KEY (flight_id, fare_class_id),
    FOREIGN KEY (flight_id) REFERENCES Dim_Flights(flight_id),
    FOREIGN KEY (fare_class_id) REFERENCES Dim_Fare_class(fare_class_id)
);