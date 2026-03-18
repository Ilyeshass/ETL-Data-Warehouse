/*
A persistent tracking table that automatically logs the success, failure, duration, and row counts of every ETL stored procedure execution.*/

USE DataWareHouse;
GO
CREATE TABLE one.etl_audit_log (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    execution_date DATETIME DEFAULT GETDATE(),
    procedure_name NVARCHAR(100),
    target_table NVARCHAR(100),
    rows_loaded INT,
    duration_seconds INT,
    status NVARCHAR(50), 
    error_message NVARCHAR(MAX) NULL
);
GO
