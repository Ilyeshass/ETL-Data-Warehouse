/*
Creating the Database structure
*/


USE master;
GO
IF EXISTS (SELECT 1 FROM sys.databases WHERE name='DataWareHouse')
BEGIN
	ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWareHouse;
END;
GO
CREATE DATABASE DataWareHouse;
GO
USE DataWareHouse
GO
CREATE SCHEMA one;
GO
CREATE SCHEMA two;
GO 
CREATE SCHEMA three;
