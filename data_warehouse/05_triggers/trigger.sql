-- tạo bảng log khi nập thông tin price vào
CREATE TABLE Price_Log (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    flight_id INT,
    fare_class_id INT,
    price DECIMAL(10,2),
    inserted_at DATETIME DEFAULT GETDATE()
);
-- lệnh chạy trigger 
CREATE TRIGGER trg_LogPriceInsert
ON Fact_Prices
AFTER INSERT
AS
BEGIN
    INSERT INTO Price_Log (flight_id, fare_class_id, price)
    SELECT flight_id, fare_class_id, price_vnd FROM inserted;
END;

-- Thêm bản ghi bình thường → sẽ GHI LOG
INSERT INTO Fact_Prices (flight_id, fare_class_id, price_vnd, date_id)
VALUES (1, 1, 500000, 20240801);

-- không cho giá vé âm
CREATE TRIGGER trg_NoNegativePrice
ON Fact_Prices
AFTER INSERT
AS
BEGIN
    IF EXISTS (SELECT 1 FROM inserted WHERE price_vnd < 0)
    BEGIN
        RAISERROR ('Giá vé không được âm.', 16, 1)
        ROLLBACK TRANSACTION
    END
END;

--Trigger chặn không cho insert giá âm
INSERT INTO Fact_Prices (flight_id, fare_class_id, price_vnd, date_id)
VALUES (2, 1, -100000, 20240801);

-- cảnh báo khi giá vé trên 10 triệu
CREATE TRIGGER trg_WarnHighPrice
ON Fact_Prices
AFTER INSERT
AS
BEGIN
    IF EXISTS (SELECT 1 FROM inserted WHERE price_vnd > 10000000)
    BEGIN
        PRINT '⚠️ Cảnh báo: Có giá vé vượt quá 10 triệu.'
    END
END;

-- ⚠️ In ra cảnh báo khi giá > 10 triệu
INSERT INTO Fact_Prices (flight_id, fare_class_id, price_vnd, date_id)
VALUES (3, 1, 12000000, 20240801);


-- tự động cập nhật log khi thêm chuyến bay
CREATE TRIGGER trg_LogPriceInsert
ON Fact_Prices
AFTER INSERT
AS
BEGIN
    INSERT INTO Price_Log(flight_id, fare_class_id, price)
    SELECT flight_id, fare_class_id, price_vnd
    FROM inserted;
END;

INSERT INTO Fact_Prices (flight_id, fare_class_id, price_vnd, date_id)
VALUES (5, 2, 350000, 20240802);


-- không cho sửa hãng bay sau khi insert 
CREATE TRIGGER trg_PreventAirlineChange
ON Dim_Flights
INSTEAD OF UPDATE
AS
BEGIN
    IF EXISTS (
        SELECT 1 FROM inserted i
        JOIN deleted d ON i.flight_id = d.flight_id
        WHERE i.airline_id <> d.airline_id
    )
    BEGIN
        RAISERROR ('Không được phép thay đổi hãng bay.', 16, 1)
    END
    ELSE
    BEGIN
        UPDATE Dim_Flights
        SET flight_date = i.flight_date,
            flight_code = i.flight_code,
            flight_number = i.flight_number,
            origin_airport_id = i.origin_airport_id,
            dest_airport_id = i.dest_airport_id
        FROM inserted i
        WHERE Dim_Flights.flight_id = i.flight_id
    END
END;

-- Giả sử flight_id = 1 đang có airline_id = 1
UPDATE Dim_Flights
SET airline_id = 2
WHERE flight_id = 1;