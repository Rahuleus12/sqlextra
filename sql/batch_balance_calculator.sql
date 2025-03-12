-- Batch Processing Version for Large Datasets
USE master;
GO

-- Create test database if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'TestDB')
BEGIN
    CREATE DATABASE TestDB;
END
GO

USE TestDB;
GO

-- First ensure we have the transactions table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[transactions]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[transactions] (
        E_NO VARCHAR(10),
        M_NO VARCHAR(10),
        [DATE] DATE,
        DEBIT DECIMAL(10,2),
        CREDIT DECIMAL(10,2),
        BALANCE DECIMAL(10,2),
        OPERATOR VARCHAR(10),
        -- Add index for better performance
        INDEX IX_transactions_main (e_no, m_no, [date])
    )
END
GO

-- Create the batch balance calculation procedure
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'update_running_balances_batch')
BEGIN
    DROP PROCEDURE [dbo].[update_running_balances_batch]
END
GO

CREATE PROCEDURE [dbo].[update_running_balances_batch]
    @batch_size INT = 100000,
    @start_date DATE = NULL,
    @end_date DATE = NULL,
    @delay_ms INT = 100  -- Delay between batches in milliseconds
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Initialize date range if not provided
    IF @start_date IS NULL
        SELECT @start_date = MIN([date]) FROM transactions;
    IF @end_date IS NULL
        SELECT @end_date = MAX([date]) FROM transactions;
    
    DECLARE @current_date DATE = @start_date;
    DECLARE @batch_end_date DATE;
    DECLARE @processed INT = 0;
    DECLARE @total_rows INT;
    DECLARE @error_count INT = 0;
    DECLARE @start_time DATETIME = GETDATE();
    
    -- Get total number of rows to process
    SELECT @total_rows = COUNT(*) 
    FROM transactions 
    WHERE [date] BETWEEN @start_date AND @end_date;
    
    -- Create a table to track progress
    CREATE TABLE #progress (
        batch_number INT IDENTITY(1,1),
        start_date DATE,
        end_date DATE,
        rows_processed INT,
        processing_time_ms INT,
        error_count INT
    );
    
    WHILE @current_date <= @end_date
    BEGIN
        BEGIN TRY
            -- Calculate batch end date
            SET @batch_end_date = DATEADD(day, 
                CASE 
                    WHEN DATEDIFF(day, @current_date, @end_date) > 30 THEN 30 
                    ELSE DATEDIFF(day, @current_date, @end_date)
                END, 
                @current_date);
            
            DECLARE @batch_start_time DATETIME = GETDATE();
            
            -- Process one batch
            UPDATE t
            SET balance = sub.running_balance
            FROM transactions t
            INNER JOIN (
                SELECT 
                    e_no, m_no, [date],
                    SUM(credit + debit) OVER (
                        PARTITION BY e_no, m_no
                        ORDER BY [date],
                                 CASE WHEN OPERATOR = 'CWO' THEN 0 ELSE 1 END,
                                 CASE WHEN credit > 0 THEN 0 ELSE 1 END
                        ROWS UNBOUNDED PRECEDING
                    ) as running_balance
                FROM transactions
                WHERE [date] BETWEEN @current_date AND @batch_end_date
            ) sub ON t.e_no = sub.e_no 
                  AND t.m_no = sub.m_no 
                  AND t.[date] = sub.[date];
            
            -- Record progress
            INSERT INTO #progress (
                start_date, 
                end_date, 
                rows_processed,
                processing_time_ms,
                error_count
            )
            VALUES (
                @current_date,
                @batch_end_date,
                @@ROWCOUNT,
                DATEDIFF(millisecond, @batch_start_time, GETDATE()),
                0
            );
            
            SET @processed = @processed + @@ROWCOUNT;
            
            -- Progress report
            RAISERROR (
                'Processed %d of %d records (%.2f%%) through date %s. Elapsed time: %d seconds', 
                0, 1, 
                @processed, 
                @total_rows,
                CAST((@processed * 100.0 / @total_rows) as DECIMAL(5,2)),
                CONVERT(VARCHAR(10), @batch_end_date, 120),
                DATEDIFF(second, @start_time, GETDATE())
            ) WITH NOWAIT;
            
            -- Optional delay between batches
            WAITFOR DELAY '00:00:00.1';
            
            -- Move to next batch
            SET @current_date = DATEADD(day, 1, @batch_end_date);
        END TRY
        BEGIN CATCH
            SET @error_count = @error_count + 1;
            
            -- Record error in progress
            INSERT INTO #progress (
                start_date, 
                end_date, 
                rows_processed,
                processing_time_ms,
                error_count
            )
            VALUES (
                @current_date,
                @batch_end_date,
                0,
                DATEDIFF(millisecond, @batch_start_time, GETDATE()),
                1
            );
            
            -- Log error details
            DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
            DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
            DECLARE @ErrorState INT = ERROR_STATE();
            
            RAISERROR (
                'Error processing batch from %s to %s: %s',
                @ErrorSeverity,
                @ErrorState,
                CONVERT(VARCHAR(10), @current_date, 120),
                CONVERT(VARCHAR(10), @batch_end_date, 120),
                @ErrorMessage
            );
            
            -- Move to next batch despite error
            SET @current_date = DATEADD(day, 1, @batch_end_date);
        END CATCH
    END
    
    -- Final summary
    SELECT 
        COUNT(*) as total_batches,
        SUM(rows_processed) as total_rows_processed,
        SUM(error_count) as total_errors,
        AVG(processing_time_ms) as avg_batch_time_ms,
        MAX(processing_time_ms) as max_batch_time_ms,
        MIN(processing_time_ms) as min_batch_time_ms
    FROM #progress;
    
    -- Cleanup
    DROP TABLE #progress;
END
GO

-- Example usage:
-- Process all transactions in batches
EXEC [dbo].[update_running_balances_batch];

-- Process specific date range
EXEC [dbo].[update_running_balances_batch] 
    @start_date = '2024-01-01',
    @end_date = '2024-12-31',
    @batch_size = 50000,
    @delay_ms = 200;

-- View results
SELECT 
    e_no,
    m_no,
    MIN([date]) as first_date,
    MAX([date]) as last_date,
    COUNT(*) as transaction_count,
    MIN(balance) as min_balance,
    MAX(balance) as max_balance
FROM transactions
GROUP BY e_no, m_no
ORDER BY e_no, m_no; 