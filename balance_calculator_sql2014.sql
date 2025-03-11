-- Balance Calculator Function for SQL Server 2014
-- This function automatically calculates running balances for transactions
-- It handles:
-- 1. Multiple accounts (E_NO, M_NO combinations)
-- 2. Multiple transactions per day
-- 3. Decimal values
-- 4. Credits and debits
-- 5. Running balances across dates

-- First, ensure we have the transactions table
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

-- Drop the procedure if it exists (SQL Server 2014 compatible way)
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'update_running_balances')
BEGIN
    DROP PROCEDURE [dbo].[update_running_balances]
END
GO

-- Create the balance calculation procedure
CREATE PROCEDURE [dbo].[update_running_balances]
AS
BEGIN
    SET NOCOUNT ON;

    -- Check if temporary table exists and drop it (SQL Server 2014 compatible way)
    IF OBJECT_ID('tempdb..#temp_ordered_transactions') IS NOT NULL
    BEGIN
        DROP TABLE #temp_ordered_transactions
    END
    
    -- Create the temporary table with ordered transactions
    SELECT 
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY e_no, m_no 
            ORDER BY [date], 
                     -- Credits come first, then debits
                     CASE WHEN credit > 0 THEN 0 ELSE 1 END,
                     -- If multiple transactions on same date, use identity column
                     (SELECT NULL)  -- SQL Server 2014 compatible way to handle row ordering
        ) as row_seq
    INTO #temp_ordered_transactions
    FROM transactions t;

    -- Declare variables for cursor
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
        -- Reset running balance for each new account
        SET @running_balance = 0
        
        -- Get max sequence for current account
        SELECT @max_seq = MAX(row_seq)
        FROM #temp_ordered_transactions
        WHERE e_no = @current_e_no 
        AND m_no = @current_m_no

        SET @current_seq = 1

        -- Process each transaction for current account
        WHILE @current_seq <= @max_seq
        BEGIN
            -- Get current transaction values
            SELECT 
                @current_credit = ISNULL(credit, 0),
                @current_debit = ISNULL(debit, 0)
            FROM #temp_ordered_transactions
            WHERE e_no = @current_e_no 
            AND m_no = @current_m_no
            AND row_seq = @current_seq

            -- Update running balance
            SET @running_balance = @running_balance + @current_credit + @current_debit

            -- Update the balance in the original transactions table
            UPDATE t
            SET balance = @running_balance
            FROM transactions t
            INNER JOIN #temp_ordered_transactions tot
            ON t.e_no = tot.e_no
            AND t.m_no = tot.m_no
            AND t.[date] = tot.[date]
            AND ISNULL(t.credit, 0) = ISNULL(tot.credit, 0)
            AND ISNULL(t.debit, 0) = ISNULL(tot.debit, 0)
            WHERE tot.e_no = @current_e_no
            AND tot.m_no = @current_m_no
            AND tot.row_seq = @current_seq

            SET @current_seq = @current_seq + 1
        END

        FETCH NEXT FROM account_cursor INTO @current_e_no, @current_m_no
    END

    CLOSE account_cursor
    DEALLOCATE account_cursor

    -- Clean up (SQL Server 2014 compatible way)
    IF OBJECT_ID('tempdb..#temp_ordered_transactions') IS NOT NULL
    BEGIN
        DROP TABLE #temp_ordered_transactions
    END
END
GO

-- Example usage:
-- 1. Insert your transactions into the transactions table
-- 2. Call the procedure to update balances:
--    EXEC update_running_balances
-- 3. View the results:
--    SELECT * FROM transactions 
--    ORDER BY e_no, m_no, [date], 
--             CASE WHEN credit > 0 THEN 0 ELSE 1 END

-- Example transaction insert:
/*
INSERT INTO transactions (E_NO, M_NO, [DATE], DEBIT, CREDIT, BALANCE, OPERATOR) VALUES 
('EMP1', '777', '2024-01-01', 0, 1500.50, 0, 'CWO'),
('EMP1', '777', '2024-01-01', 750.25, 0, 0, NULL)
*/

-- To test the procedure:
-- 1. INSERT your transactions
-- 2. Run: EXEC update_running_balances
-- 3. View results: SELECT * FROM transactions ORDER BY e_no, m_no, [date] 