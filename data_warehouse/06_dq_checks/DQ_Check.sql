CREATE TABLE dq_log (
    check_id INT IDENTITY(1,1) PRIMARY KEY,
    check_time DATETIME DEFAULT GETDATE(),
    table_name VARCHAR(100),
    check_type VARCHAR(100),
    column_name VARCHAR(100),
    result VARCHAR(100),
    row_count INT,
    message NVARCHAR(500)
);

-- Kiểm tra các dòng trùng lặp theo: flight_id, fare_class, fare_code, price_vnd
DECLARE @dup_count INT = (
    SELECT COUNT(*) FROM (
        SELECT flight_id, fare_class_id, price_vnd, COUNT(*) AS cnt
        FROM Fact_Prices
        GROUP BY flight_id, fare_class_id, price_vnd
        HAVING COUNT(*) > 1
    ) AS dup
);

-- Log kết quả vào bảng dq_log
INSERT INTO dq_log (table_name, check_type, result, row_count, message)
VALUES ('Fact_Prices', 'Duplicate Check', 
        CASE WHEN @dup_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        @dup_count,
        'Số dòng bị trùng theo flight_id + fare_class  + price_vnd');
-- Kiểm tra cột price_vnd trong Fact_Prices không được NULL
DECLARE @null_count INT = (
    SELECT COUNT(*) FROM Fact_Prices WHERE price_vnd IS NULL
);

INSERT INTO dq_log (table_name, check_type, column_name, result, row_count, message)
VALUES ('Fact_Prices', 'Null Check', 'price_vnd', 
        CASE WHEN @null_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        @null_count, 
        'Số dòng price_vnd bị NULL');

-- Fact_Prices.flight_id phải tồn tại trong Dim_Flights.flight_id
DECLARE @orphan_count INT = (
    SELECT COUNT(*) FROM Fact_Prices fp
    LEFT JOIN Dim_Flights df ON fp.flight_id = df.flight_id
    WHERE df.flight_id IS NULL
);

INSERT INTO dq_log (table_name, check_type, column_name, result, row_count, message)
VALUES ('Fact_Prices', 'FK Orphan Check', 'flight_id',
        CASE WHEN @orphan_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        @orphan_count,
        'Số dòng Fact_Prices có flight_id không tồn tại ở Dim_Flights');

--
-- Kiểm tra nhiều cột null  trong Fact_Prices
DECLARE @cols TABLE (col_name VARCHAR(100));
INSERT INTO @cols (col_name)
VALUES ('flight_id'), ('fare_code'), ('price_vnd'), ('taxes');

DECLARE @colname VARCHAR(100), @sql NVARCHAR(MAX);

DECLARE col_cursor CURSOR FOR SELECT col_name FROM @cols;
OPEN col_cursor;
FETCH NEXT FROM col_cursor INTO @colname;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = '
    DECLARE @cnt INT = (SELECT COUNT(*) FROM Fact_Prices WHERE [' + @colname + '] IS NULL);
    INSERT INTO dq_log (table_name, check_type, column_name, result, row_count, message)
    VALUES (''Fact_Prices'', ''Null Check'', ''' + @colname + ''', 
            CASE WHEN @cnt = 0 THEN ''PASS'' ELSE ''FAIL'' END,
            @cnt, ''Cột [' + @colname + '] bị NULL'');
    ';
    EXEC sp_executesql @sql;

    FETCH NEXT FROM col_cursor INTO @colname;
END
CLOSE col_cursor;
DEALLOCATE col_cursor;

--Xem tất cả kết quả:
SELECT * FROM dq_log ORDER BY check_time DESC;