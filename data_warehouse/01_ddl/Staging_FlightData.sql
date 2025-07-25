DROP TABLE IF EXISTS dbo.Staging_Raw;
GO
CREATE TABLE dbo.Staging_Raw (
    [date] NVARCHAR(20),
    [route] NVARCHAR(50),
    [flight_id] NVARCHAR(100),
    [origin] NVARCHAR(10),
    [destination] NVARCHAR(10),
    [fare_class] NVARCHAR(10),
    [fare_code] NVARCHAR(50),
    [price_vnd] NVARCHAR(50),
    [taxes] NVARCHAR(50),
    [booking_class] NVARCHAR(10),
    [cabin] NVARCHAR(20),
    [quota] NVARCHAR(50),
    [service_code] NVARCHAR(50),
    [fare_conditions] NVARCHAR(200),
    [operating_airline_code] NVARCHAR(10),
    [operating_airline_name] NVARCHAR(50),
    [airline] NVARCHAR(50),
    [flight_date] NVARCHAR(20),
    [flight_code] NVARCHAR(20),
    [flight_number] NVARCHAR(20),  -- để text
    [year] NVARCHAR(10),
    [month] NVARCHAR(10),
    [day] NVARCHAR(10),
    [dow] NVARCHAR(10),
    [is_weekend] NVARCHAR(10),
    [city] NVARCHAR(50)
);
GO

BULK INSERT dbo.Staging_Raw
FROM '/var/opt/mssql/data_sql.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',     -- nếu vẫn lỗi, thử '0x0d0a'
    FIELDQUOTE = '"',
    TABLOCK,
    MAXERRORS = 0               -- cho import hết rồi lọc sau
);
GO
