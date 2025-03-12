-- SQL Server 2014 compatible version
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

-- Set compatibility level to SQL Server 2014
ALTER DATABASE TestDB SET COMPATIBILITY_LEVEL = 120;
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
        OPERATOR VARCHAR(10)
    )
END
GO

-- Create or alter the balance calculation procedure
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'update_running_balances')
BEGIN
    DROP PROCEDURE [dbo].[update_running_balances]
END
GO

CREATE PROCEDURE [dbo].[update_running_balances]
AS
BEGIN
    SET NOCOUNT ON;

    -- Check if temporary table exists and drop it
    IF OBJECT_ID('tempdb..#temp_ordered_transactions') IS NOT NULL
    BEGIN
        DROP TABLE #temp_ordered_transactions
    END
    
    -- Create temporary table with ordered transactions
    -- Using simpler ORDER BY clause for SQL Server 2014 compatibility
    SELECT 
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY e_no, m_no 
            ORDER BY [date], 
                     CASE WHEN OPERATOR = 'CWO' THEN 0 ELSE 1 END,
                     CASE WHEN credit > 0 THEN 0 ELSE 1 END
        ) as row_seq
    INTO #temp_ordered_transactions
    FROM transactions t;

    -- Process each account
    DECLARE @current_e_no VARCHAR(10)
    DECLARE @current_m_no VARCHAR(10)
    DECLARE @running_balance DECIMAL(10,2)
    DECLARE @max_seq INT
    DECLARE @current_seq INT
    DECLARE @current_credit DECIMAL(10,2)
    DECLARE @current_debit DECIMAL(10,2)

    -- Cursor for processing each account
    DECLARE account_cursor CURSOR LOCAL FAST_FORWARD FOR 
        SELECT DISTINCT e_no, m_no 
        FROM #temp_ordered_transactions 
        ORDER BY e_no, m_no

    OPEN account_cursor
    FETCH NEXT FROM account_cursor INTO @current_e_no, @current_m_no

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @running_balance = 0
        
        SELECT @max_seq = MAX(row_seq)
        FROM #temp_ordered_transactions
        WHERE e_no = @current_e_no 
        AND m_no = @current_m_no

        SET @current_seq = 1

        WHILE @current_seq <= @max_seq
        BEGIN
            SELECT 
                @current_credit = ISNULL(credit, 0),
                @current_debit = ISNULL(debit, 0)
            FROM #temp_ordered_transactions
            WHERE e_no = @current_e_no 
            AND m_no = @current_m_no
            AND row_seq = @current_seq

            SET @running_balance = @running_balance + @current_credit + @current_debit

            UPDATE t
            SET balance = @running_balance
            FROM transactions t
            INNER JOIN #temp_ordered_transactions tot
            ON t.e_no = tot.e_no
            AND t.m_no = tot.m_no
            AND t.[date] = tot.[date]
            AND ISNULL(t.credit, 0) = ISNULL(tot.credit, 0)
            AND ISNULL(t.debit, 0) = ISNULL(tot.debit, 0)
            AND ISNULL(t.operator, '') = ISNULL(tot.operator, '')
            WHERE tot.e_no = @current_e_no
            AND tot.m_no = @current_m_no
            AND tot.row_seq = @current_seq

            SET @current_seq = @current_seq + 1
        END

        FETCH NEXT FROM account_cursor INTO @current_e_no, @current_m_no
    END

    CLOSE account_cursor
    DEALLOCATE account_cursor

    -- Clean up
    IF OBJECT_ID('tempdb..#temp_ordered_transactions') IS NOT NULL
    BEGIN
        DROP TABLE #temp_ordered_transactions
    END
END
GO

-- Clear existing test data
DELETE FROM [dbo].[transactions];
GO

-- Insert test data
INSERT INTO [dbo].[transactions] (E_NO, M_NO, [DATE], DEBIT, CREDIT, BALANCE, OPERATOR) VALUES 
-- Test case 1: Single account, multiple transactions same day
('EMP1', '111', '2014-01-01', 0, 1000.00, 0, 'CWO'),
('EMP1', '111', '2014-01-01', 0, 200.00, 0, NULL),
('EMP1', '111', '2014-01-01', 500.00, 0, 0, NULL),

-- Test case 2: Single account, transactions on different dates
('EMP2', '222', '2014-01-01', 0, 2000.00, 0, 'CWO'),
('EMP2', '222', '2014-01-02', 300.00, 0, 0, NULL),
('EMP2', '222', '2014-01-03', 0, 500.00, 0, NULL),

-- Test case 3: Multiple accounts with overlapping dates
('EMP3', '333', '2014-01-01', 0, 3000.00, 0, 'CWO'),
('EMP3', '333', '2014-01-02', 1000.00, 0, 0, NULL),
('EMP4', '444', '2014-01-01', 0, 5000.00, 0, 'CWO'),
('EMP4', '444', '2014-01-02', 2000.00, 0, 0, NULL);
GO

-- Run the balance calculation
EXEC [dbo].[update_running_balances];
GO

-- View results with verification
SELECT 
    E_NO,
    M_NO,
    [DATE],
    DEBIT,
    CREDIT,
    BALANCE,
    OPERATOR,
    -- Add a column to show expected balance for verification
    CASE 
        WHEN E_NO = 'EMP1' AND M_NO = '111' THEN 
            CASE ROW_NUMBER() OVER (PARTITION BY E_NO, M_NO ORDER BY [DATE], CASE WHEN OPERATOR = 'CWO' THEN 0 ELSE 1 END, CASE WHEN CREDIT > 0 THEN 0 ELSE 1 END)
                WHEN 1 THEN 1000.00
                WHEN 2 THEN 1200.00
                WHEN 3 THEN 1700.00
            END
        WHEN E_NO = 'EMP2' AND M_NO = '222' THEN
            CASE ROW_NUMBER() OVER (PARTITION BY E_NO, M_NO ORDER BY [DATE], CASE WHEN OPERATOR = 'CWO' THEN 0 ELSE 1 END, CASE WHEN CREDIT > 0 THEN 0 ELSE 1 END)
                WHEN 1 THEN 2000.00
                WHEN 2 THEN 2300.00
                WHEN 3 THEN 2800.00
            END
        WHEN E_NO = 'EMP3' AND M_NO = '333' THEN
            CASE ROW_NUMBER() OVER (PARTITION BY E_NO, M_NO ORDER BY [DATE], CASE WHEN OPERATOR = 'CWO' THEN 0 ELSE 1 END, CASE WHEN CREDIT > 0 THEN 0 ELSE 1 END)
                WHEN 1 THEN 3000.00
                WHEN 2 THEN 4000.00
            END
        WHEN E_NO = 'EMP4' AND M_NO = '444' THEN
            CASE ROW_NUMBER() OVER (PARTITION BY E_NO, M_NO ORDER BY [DATE], CASE WHEN OPERATOR = 'CWO' THEN 0 ELSE 1 END, CASE WHEN CREDIT > 0 THEN 0 ELSE 1 END)
                WHEN 1 THEN 5000.00
                WHEN 2 THEN 7000.00
            END
    END as ExpectedBalance,
    -- Add a column to show if the balance matches expected
    CASE 
        WHEN BALANCE = CASE 
            WHEN E_NO = 'EMP1' AND M_NO = '111' THEN 
                CASE ROW_NUMBER() OVER (PARTITION BY E_NO, M_NO ORDER BY [DATE], CASE WHEN OPERATOR = 'CWO' THEN 0 ELSE 1 END, CASE WHEN CREDIT > 0 THEN 0 ELSE 1 END)
                    WHEN 1 THEN 1000.00
                    WHEN 2 THEN 1200.00
                    WHEN 3 THEN 1700.00
                END
            WHEN E_NO = 'EMP2' AND M_NO = '222' THEN
                CASE ROW_NUMBER() OVER (PARTITION BY E_NO, M_NO ORDER BY [DATE], CASE WHEN OPERATOR = 'CWO' THEN 0 ELSE 1 END, CASE WHEN CREDIT > 0 THEN 0 ELSE 1 END)
                    WHEN 1 THEN 2000.00
                    WHEN 2 THEN 2300.00
                    WHEN 3 THEN 2800.00
                END
            WHEN E_NO = 'EMP3' AND M_NO = '333' THEN
                CASE ROW_NUMBER() OVER (PARTITION BY E_NO, M_NO ORDER BY [DATE], CASE WHEN OPERATOR = 'CWO' THEN 0 ELSE 1 END, CASE WHEN CREDIT > 0 THEN 0 ELSE 1 END)
                    WHEN 1 THEN 3000.00
                    WHEN 2 THEN 4000.00
                END
            WHEN E_NO = 'EMP4' AND M_NO = '444' THEN
                CASE ROW_NUMBER() OVER (PARTITION BY E_NO, M_NO ORDER BY [DATE], CASE WHEN OPERATOR = 'CWO' THEN 0 ELSE 1 END, CASE WHEN CREDIT > 0 THEN 0 ELSE 1 END)
                    WHEN 1 THEN 5000.00
                    WHEN 2 THEN 7000.00
                END
        END THEN 'PASS'
        ELSE 'FAIL'
    END as TestResult
FROM [dbo].[transactions]
ORDER BY E_NO, M_NO, [DATE], 
         CASE WHEN OPERATOR = 'CWO' THEN 0 ELSE 1 END,
         CASE WHEN CREDIT > 0 THEN 0 ELSE 1 END;
GO 