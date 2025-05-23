-- Production Balance Calculator for SQL Server 2014
-- This script will work with an existing cwizbank_adani database and MEMBER1 table

USE cwizbank_adani;
GO

-- First verify the table structure
SELECT COLUMN_NAME as column_name,
       DATA_TYPE as data_type,
       CHARACTER_MAXIMUM_LENGTH as max_length,
       NUMERIC_PRECISION as precision,
       NUMERIC_SCALE as scale,
       IS_NULLABLE as is_nullable
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'MEMBER1';
GO

-- Create index for better performance
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_MEMBER1_Account_Date' AND object_id = OBJECT_ID('[dbo].[MEMBER1]'))
BEGIN
    CREATE INDEX IX_MEMBER1_Account_Date ON [dbo].[MEMBER1] (E_NO, M_NO, [DATE]);
END
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
    
    BEGIN TRY
        -- Create temporary table with ordered transactions
        IF OBJECT_ID('tempdb..#temp_ordered_transactions') IS NOT NULL
            DROP TABLE #temp_ordered_transactions;

        SELECT 
            t.*,
            ROW_NUMBER() OVER (
                PARTITION BY M_NO 
                ORDER BY [DATE], 
                         CASE WHEN OPERATOR = 'CWO' THEN 0 ELSE 1 END,
                         CASE WHEN CREDIT IS NOT NULL THEN 0 ELSE 1 END
            ) as row_seq
        INTO #temp_ordered_transactions
        FROM [dbo].[MEMBER1] t;

        -- Process each member
        DECLARE @current_m_no VARCHAR(10)
        DECLARE @running_balance DECIMAL(10,2)
        DECLARE @max_seq INT
        DECLARE @current_seq INT
        DECLARE @current_credit DECIMAL(10,2)
        DECLARE @current_debit DECIMAL(10,2)
        DECLARE @earliest_date DATE
        DECLARE @latest_date DATE
        DECLARE @total_credits DECIMAL(10,2) = 0
        DECLARE @total_debits DECIMAL(10,2) = 0
        DECLARE @accounts_processed INT = 0

        -- Cursor for processing each member
        DECLARE member_cursor CURSOR LOCAL FAST_FORWARD FOR 
            SELECT DISTINCT M_NO 
            FROM #temp_ordered_transactions 
            ORDER BY M_NO;

        OPEN member_cursor
        FETCH NEXT FROM member_cursor INTO @current_m_no

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @running_balance = 0
            
            SELECT @max_seq = MAX(row_seq)
            FROM #temp_ordered_transactions
            WHERE M_NO = @current_m_no

            SET @current_seq = 1

            WHILE @current_seq <= @max_seq
            BEGIN
                SELECT 
                    @current_credit = ISNULL(CREDIT, 0),
                    @current_debit = ISNULL(DEBIT, 0)
                FROM #temp_ordered_transactions
                WHERE M_NO = @current_m_no
                AND row_seq = @current_seq

                SET @running_balance = @running_balance + @current_credit - @current_debit

                UPDATE t
                SET BALANCE = @running_balance
                FROM [dbo].[MEMBER1] t
                INNER JOIN #temp_ordered_transactions tot
                ON t.M_NO = tot.M_NO
                AND t.[DATE] = tot.[DATE]
                AND ISNULL(t.CREDIT, 0) = ISNULL(tot.CREDIT, 0)
                AND ISNULL(t.DEBIT, 0) = ISNULL(tot.DEBIT, 0)
                AND ISNULL(t.OPERATOR, '') = ISNULL(tot.OPERATOR, '')
                WHERE tot.M_NO = @current_m_no
                AND tot.row_seq = @current_seq

                SET @current_seq = @current_seq + 1
            END

            SET @accounts_processed = @accounts_processed + 1
            
            -- Accumulate totals for this member
            SELECT 
                @total_credits = @total_credits + ISNULL(SUM(CREDIT), 0),
                @total_debits = @total_debits + ISNULL(SUM(DEBIT), 0)
            FROM #temp_ordered_transactions
            WHERE M_NO = @current_m_no;

            FETCH NEXT FROM member_cursor INTO @current_m_no
        END

        CLOSE member_cursor
        DEALLOCATE member_cursor

        -- Get date range
        SELECT 
            @earliest_date = MIN([DATE]),
            @latest_date = MAX([DATE])
        FROM [dbo].[MEMBER1];

        -- Return summary
        SELECT 
            'Balance calculation complete' as step,
            @accounts_processed as accounts_processed,
            COUNT(*) as total_records,
            @earliest_date as earliest_date,
            @latest_date as latest_date,
            @total_credits as total_credits,
            @total_debits as total_debits,
            @total_credits - @total_debits as net_balance
        FROM [dbo].[MEMBER1];

    END TRY
    BEGIN CATCH
        SELECT 
            ERROR_NUMBER() AS ErrorNumber,
            ERROR_MESSAGE() AS ErrorMessage;
    END CATCH
END;
GO

-- Create procedure to mark opening balances
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'mark_member1_opening_balances')
BEGIN
    DROP PROCEDURE [dbo].[mark_member1_opening_balances]
END
GO

CREATE PROCEDURE [dbo].[mark_member1_opening_balances]
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        -- Mark the first transaction for each member as CWO
        UPDATE t
        SET OPERATOR = 'CWO'
        FROM [dbo].[MEMBER1] t
        INNER JOIN (
            SELECT M_NO, MIN([DATE]) as first_date
            FROM [dbo].[MEMBER1]
            GROUP BY M_NO
        ) f ON t.M_NO = f.M_NO 
            AND t.[DATE] = f.first_date;

        -- Return summary
        SELECT 'Opening balances marked' as step,
               COUNT(*) as records_marked,
               COUNT(DISTINCT M_NO) as accounts_marked
        FROM [dbo].[MEMBER1]
        WHERE OPERATOR = 'CWO';
    END TRY
    BEGIN CATCH
        SELECT 
            ERROR_NUMBER() AS ErrorNumber,
            ERROR_MESSAGE() AS ErrorMessage;
    END CATCH
END;
GO

-- Create procedure to verify balances
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'verify_member1_balances')
BEGIN
    DROP PROCEDURE [dbo].[verify_member1_balances]
END
GO

CREATE PROCEDURE [dbo].[verify_member1_balances]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Verify balances
    WITH BalanceCheck AS (
        SELECT 
            M_NO,
            [DATE],
            DEBIT,
            CREDIT,
            BALANCE,
            LAG(BALANCE) OVER (PARTITION BY M_NO ORDER BY [DATE], 
                CASE WHEN OPERATOR = 'CWO' THEN 0 ELSE 1 END,
                CASE WHEN CREDIT IS NOT NULL THEN 0 ELSE 1 END) as prev_balance,
            ISNULL(CREDIT, 0) - ISNULL(DEBIT, 0) as expected_change
        FROM [dbo].[MEMBER1]
    )
    SELECT 
        'Balance verification complete' as step,
        COUNT(*) as total_records,
        COUNT(DISTINCT M_NO) as total_accounts,
        SUM(CASE WHEN OPERATOR = 'CWO' THEN 1 ELSE 0 END) as total_cwo_records,
        SUM(CASE WHEN CREDIT IS NOT NULL THEN 1 ELSE 0 END) as total_credit_transactions,
        SUM(CASE WHEN DEBIT IS NOT NULL THEN 1 ELSE 0 END) as total_debit_transactions,
        SUM(ISNULL(CREDIT, 0)) as total_credits,
        SUM(ISNULL(DEBIT, 0)) as total_debits,
        SUM(ISNULL(CREDIT, 0)) - SUM(ISNULL(DEBIT, 0)) as net_balance
    FROM [dbo].[MEMBER1]
    WHERE M_NO IN (
        SELECT M_NO 
        FROM BalanceCheck 
        WHERE ABS(ISNULL(BALANCE - prev_balance, 0) - expected_change) > 0.01
    );
END
GO

-- Print current table structure
SELECT 
    c.name as column_name,
    t.name as data_type,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE object_id = OBJECT_ID('[dbo].[MEMBER1]')
ORDER BY c.column_id;

-- Instructions for use:
PRINT '
To process balances in your MEMBER1 table:

1. First mark opening balances:
   EXEC [dbo].[mark_member1_opening_balances];

2. Then calculate all balances:
   EXEC [dbo].[update_member1_balances];

3. Finally verify the results:
   EXEC [dbo].[verify_member1_balances];

Note: This script assumes your MEMBER1 table has at minimum these columns:
- E_NO (VARCHAR)
- M_NO (VARCHAR)
- DATE (DATE or DATETIME)
- DEBIT (DECIMAL)
- CREDIT (DECIMAL)
- BALANCE (DECIMAL)
- OPERATOR (VARCHAR)

Additional columns in your table will not affect the balance calculation.
'; 