RESTORE FILELISTONLY 
FROM DISK = N'/var/opt/mssql/backup/DU_AN_BACKUP.bak';

RESTORE DATABASE FlightPricingDB
FROM DISK = N'/var/opt/mssql/backup/DU_AN_BACKUP.bak'
WITH MOVE 'FlightPricingDB' TO '/var/opt/mssql/data/FlightPricingDB.mdf',
     MOVE 'FlightPricingDB_log' TO '/var/opt/mssql/data/FlightPricingDB_log.ldf',
     REPLACE;

RESTORE DATABASE FlightPricingDB WITH RECOVERY;
