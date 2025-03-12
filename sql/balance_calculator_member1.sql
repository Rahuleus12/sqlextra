-- Balance Calculator for cwizbank_adani.dbo.MEMBER1
USE cwizbank_adani;
GO

-- Create or alter the balance calculation procedure
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'update_member1_balances')
BEGIN
    DROP PROCEDURE [dbo].[update_member1_balances]
END
GO

CREATE PROCEDURE [dbo].[update_member1_balances]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check if temporary table exists and drop it
    IF OBJECT_ID('tempdb..#temp_ordered_transactions') IS NOT NULL
    BEGIN
        DROP TABLE #temp_ordered_transactions
    END
    
    -- Create temporary table with ordered transactions
    -- Assuming E_NO and M_NO are the employee and member numbers in your table
    SELECT 
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY E_NO, M_NO 
            ORDER BY [DATE], 
                     CASE WHEN OPERATOR = 'CWO' THEN 0 ELSE 1 END,
                     CASE WHEN CREDIT > 0 THEN 0 ELSE 1 END
        ) as row_seq
    INTO #temp_ordered_transactions
    FROM MEMBER1 t;

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
        SELECT DISTINCT E_NO, M_NO 
        FROM #temp_ordered_transactions 
        ORDER BY E_NO, M_NO

    OPEN account_cursor
    FETCH NEXT FROM account_cursor INTO @current_e_no, @current_m_no

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @running_balance = 0
        
        SELECT @max_seq = MAX(row_seq)
        FROM #temp_ordered_transactions
        WHERE E_NO = @current_e_no 
        AND M_NO = @current_m_no

        SET @current_seq = 1

        WHILE @current_seq <= @max_seq
        BEGIN
            SELECT 
                @current_credit = ISNULL(CREDIT, 0),
                @current_debit = ISNULL(DEBIT, 0)
            FROM #temp_ordered_transactions
            WHERE E_NO = @current_e_no 
            AND M_NO = @current_m_no
            AND row_seq = @current_seq

            SET @running_balance = @running_balance + @current_credit + @current_debit

            UPDATE t
            SET BALANCE = @running_balance
            FROM MEMBER1 t
            INNER JOIN #temp_ordered_transactions tot
            ON t.E_NO = tot.E_NO
            AND t.M_NO = tot.M_NO
            AND t.[DATE] = tot.[DATE]
            AND ISNULL(t.CREDIT, 0) = ISNULL(tot.CREDIT, 0)
            AND ISNULL(t.DEBIT, 0) = ISNULL(tot.DEBIT, 0)
            AND ISNULL(t.OPERATOR, '') = ISNULL(tot.OPERATOR, '')
            WHERE tot.E_NO = @current_e_no
            AND tot.M_NO = @current_m_no
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

-- Create a procedure to mark opening balances if needed
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'mark_member1_opening_balances')
BEGIN
    DROP PROCEDURE [dbo].[mark_member1_opening_balances]
END
GO

CREATE PROCEDURE [dbo].[mark_member1_opening_balances]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Mark the first transaction for each account as CWO
    UPDATE t
    SET OPERATOR = 'CWO'
    FROM MEMBER1 t
    INNER JOIN (
        SELECT E_NO, M_NO, MIN([DATE]) as first_date
        FROM MEMBER1
        GROUP BY E_NO, M_NO
    ) f ON t.E_NO = f.E_NO 
        AND t.M_NO = f.M_NO 
        AND t.[DATE] = f.first_date
    WHERE t.OPERATOR IS NULL OR t.OPERATOR <> 'CWO';
END
GO

-- Create a procedure to verify balances
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'verify_member1_balances')
BEGIN
    DROP PROCEDURE [dbo].[verify_member1_balances]
END
GO

CREATE PROCEDURE [dbo].[verify_member1_balances]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check for accounts missing CWO
    SELECT E_NO, M_NO, MIN([DATE]) as first_date, 
           MAX(CASE WHEN OPERATOR = 'CWO' THEN 1 ELSE 0 END) as has_cwo
    FROM MEMBER1
    GROUP BY E_NO, M_NO
    HAVING MAX(CASE WHEN OPERATOR = 'CWO' THEN 1 ELSE 0 END) = 0;
    
    -- Check for invalid amounts
    SELECT *
    FROM MEMBER1
    WHERE (DEBIT IS NULL AND CREDIT IS NULL)
    OR (DEBIT < 0 OR CREDIT < 0);
    
    -- Verify balance progression
    WITH BalanceChanges AS (
        SELECT 
            E_NO,
            M_NO,
            [DATE],
            DEBIT,
            CREDIT,
            BALANCE,
            LAG(BALANCE) OVER (PARTITION BY E_NO, M_NO ORDER BY [DATE]) as prev_balance,
            CREDIT + DEBIT as expected_change
        FROM MEMBER1
    )
    SELECT *
    FROM BalanceChanges
    WHERE ABS((BALANCE - ISNULL(prev_balance, 0)) - expected_change) > 0.01;
END
GO

-- Example usage:
/*
-- Step 1: Mark opening balances (if needed)
EXEC [dbo].[mark_member1_opening_balances];

-- Step 2: Calculate balances
EXEC [dbo].[update_member1_balances];

-- Step 3: Verify results
EXEC [dbo].[verify_member1_balances];

-- View results for specific account
SELECT 
    E_NO,
    M_NO,
    [DATE],
    DEBIT,
    CREDIT,
    BALANCE,
    OPERATOR
FROM MEMBER1
WHERE E_NO = 'your_employee_no'
AND M_NO = 'your_member_no'
ORDER BY [DATE];

-- View summary by account
SELECT 
    E_NO,
    M_NO,
    MIN([DATE]) as first_date,
    MAX([DATE]) as last_date,
    COUNT(*) as transaction_count,
    MIN(BALANCE) as min_balance,
    MAX(BALANCE) as max_balance
FROM MEMBER1
GROUP BY E_NO, M_NO
ORDER BY E_NO, M_NO;
*/

CREATE TABLE [dbo].[MEMBER1] (
    E_NO VARCHAR(10) NOT NULL,
    M_NO VARCHAR(10) NOT NULL,
    [DATE] DATE NOT NULL,
    DEBIT DECIMAL(10,2) NULL,
    CREDIT DECIMAL(10,2) NULL,
    BALANCE DECIMAL(10,2) NULL,
    OPERATOR VARCHAR(10) NULL,
    CONSTRAINT PK_MEMBER1 PRIMARY KEY CLUSTERED (E_NO, M_NO, [DATE])
) 