-- Kiểm tra trong Landing
USE [DW_Synthea_Landing];
GO
SELECT dp.name AS DatabaseUser, 
       sp.name AS LoginName,
       dp.type_desc
FROM sys.database_principals dp
LEFT JOIN sys.server_principals sp ON dp.sid = sp.sid
WHERE dp.name LIKE '%kadfw%' OR sp.name LIKE '%kadfw%';

-- Kiểm tra trong Staging
USE [DW_Synthea_Staging];
GO
SELECT dp.name AS DatabaseUser, 
       sp.name AS LoginName,
       dp.type_desc
FROM sys.database_principals dp
LEFT JOIN sys.server_principals sp ON dp.sid = sp.sid
WHERE dp.name LIKE '%kadfw%' OR sp.name LIKE '%kadfw%';