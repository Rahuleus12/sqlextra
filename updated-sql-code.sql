-- Loan Balance Calculator for SQL Server 2014
-- Updated to work with loan accounts and principle/interest tracking

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
WHERE TABLE_NAME = 'MEMBER_LOANS';
GO

-- Create index for better performance
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_MEMBER_LOANS_Account_Loan_Date' AND object_id = OBJECT_ID('[dbo].[MEMBER_LOANS]'))
BEGIN
    CREATE INDEX IX_MEMBER_LOANS_Account_Loan_Date ON [dbo].[MEMBER_LOANS] (M_NO, LOAN_NO, [DATE]);
END
GO

-- Create or alter the balance calculation procedure
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'update_loan_balances')
BEGIN
    DROP PROCEDURE [dbo].[update_loan_balances]
END
GO

CREATE PROCEDURE [dbo].[update_loan_balances]
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
                PARTITION BY M_NO, LOAN_NO
                ORDER BY [DATE], 
                         CASE WHEN OPERATOR = 'CWO' THEN 0 ELSE 1 END,
                         CASE WHEN (PRINCIPLE IS NOT NULL OR INTEREST IS NOT NULL) THEN 0 ELSE 1 END
            ) as row_seq
        INTO #temp_ordered_transactions
        FROM [dbo].[MEMBER_LOANS] t;

        -- Process each member-loan combination
        DECLARE @current_m_no VARCHAR(10)
        DECLARE @current_loan_no VARCHAR(10)
        DECLARE @running_balance DECIMAL(10,2)
        DECLARE @max_seq INT
        DECLARE @current_seq INT
        DECLARE @current_principle DECIMAL(10,2)
        DECLARE @current_interest DECIMAL(10,2)
        DECLARE @current_debit DECIMAL(10,2)
        DECLARE @earliest_date DATE
        DECLARE @latest_date DATE
        DECLARE @total_principles DECIMAL(10,2) = 0
        DECLARE @total_interests DECIMAL(10,2) = 0
        DECLARE @total_debits DECIMAL(10,2) = 0
        DECLARE @loans_processed INT = 0

        -- Cursor for processing each member-loan
        DECLARE loan_cursor CURSOR LOCAL FAST_FORWARD FOR 
            SELECT DISTINCT M_NO, LOAN_NO
            FROM #temp_ordered_transactions 
            ORDER BY M_NO, LOAN_NO;

        OPEN loan_cursor
        FETCH NEXT FROM loan_cursor INTO @current_m_no, @current_loan_no

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @running_balance = 0
            
            SELECT @max_seq = MAX(row_seq)
            FROM #temp_ordered_transactions
            WHERE M_NO = @current_m_no
            AND LOAN_NO = @current_loan_no

            SET @current_seq = 1

            WHILE @current_seq <= @max_seq
            BEGIN
                SELECT 
                    @current_principle = ISNULL(PRINCIPLE, 0),
                    @current_interest = ISNULL(INTEREST, 0),
                    @current_debit = ISNULL(DEBIT, 0)
                FROM #temp_ordered_transactions
                WHERE M_NO = @current_m_no
                AND LOAN_NO = @current_loan_no
                AND row_seq = @current_seq

                SET @running_balance = @running_balance + @current_principle + @current_interest - @current_debit

                UPDATE t
                SET BALANCE = @running_balance,
                    -- Calculate TOTAL as sum of PRINCIPLE and INTEREST
                    TOTAL = ISNULL(PRINCIPLE, 0) + ISNULL(INTEREST, 0)
                FROM [dbo].[MEMBER_LOANS] t
                INNER JOIN #temp_ordered_transactions tot
                ON t.M_NO = tot.M_NO
                AND t.LOAN_NO = tot.LOAN_NO
                AND t.[DATE] = tot.[DATE]
                AND ISNULL(t.PRINCIPLE, 0) = ISNULL(tot.PRINCIPLE, 0)
                AND ISNULL(t.INTEREST, 0) = ISNULL(tot.INTEREST, 0)
                AND ISNULL(t.DEBIT, 0) = ISNULL(tot.DEBIT, 0)
                AND ISNULL(t.OPERATOR, '') = ISNULL(tot.OPERATOR, '')
                WHERE tot.M_NO = @current_m_no
                AND tot.LOAN_NO = @current_loan_no
                AND tot.row_seq = @current_seq

                SET @current_seq = @current_seq + 1
            END

            SET @loans_processed = @loans_processed + 1
            
            -- Accumulate totals for this loan
            SELECT 
                @total_principles = @total_principles + ISNULL(SUM(PRINCIPLE), 0),
                @total_interests = @total_interests + ISNULL(SUM(INTEREST), 0),
                @total_debits = @total_debits + ISNULL(SUM(DEBIT), 0)
            FROM #temp_ordered_transactions
            WHERE M_NO = @current_m_no
            AND LOAN_NO = @current_loan_no;

            FETCH NEXT FROM loan_cursor INTO @current_m_no, @current_loan_no
        END

        CLOSE loan_cursor
        DEALLOCATE loan_cursor

        -- Get date range
        SELECT 
            @earliest_date = MIN([DATE]),
            @latest_date = MAX([DATE])
        FROM [dbo].[MEMBER_LOANS];

        -- Return summary
        SELECT 
            'Balance calculation complete' as step,
            @loans_processed as loans_processed,
            COUNT(*) as total_records,
            @earliest_date as earliest_date,
            @latest_date as latest_date,
            @total_principles as total_principles,
            @total_interests as total_interests,
            @total_debits as total_debits,
            (@total_principles + @total_interests) - @total_debits as net_balance
        FROM [dbo].[MEMBER_LOANS];

    END TRY
    BEGIN CATCH
        SELECT 
            ERROR_NUMBER() AS ErrorNumber,
            ERROR_MESSAGE() AS ErrorMessage;
    END CATCH
END;
GO

-- Create procedure to mark opening balances
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'mark_loan_opening_balances')
BEGIN
    DROP PROCEDURE [dbo].[mark_loan_opening_balances]
END
GO

CREATE PROCEDURE [dbo].[mark_loan_opening_balances]
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        -- Mark the first transaction for each member-loan as CWO
        UPDATE t
        SET OPERATOR = 'CWO'
        FROM [dbo].[MEMBER_LOANS] t
        INNER JOIN (
            SELECT M_NO, LOAN_NO, MIN([DATE]) as first_date
            FROM [dbo].[MEMBER_LOANS]
            GROUP BY M_NO, LOAN_NO
        ) f ON t.M_NO = f.M_NO 
            AND t.LOAN_NO = f.LOAN_NO
            AND t.[DATE] = f.first_date;

        -- Return summary
        SELECT 'Opening balances marked' as step,
               COUNT(*) as records_marked,
               COUNT(DISTINCT M_NO) as accounts_marked,
               COUNT(DISTINCT LOAN_NO) as loans_marked
        FROM [dbo].[MEMBER_LOANS]
        WHERE OPERATOR = 'CWO';
    END TRY
    BEGIN CATCH
        SELECT 
            ERROR_NUMBER() AS ErrorNumber,
            ERROR_MESSAGE() AS ErrorMessage;
    END CATCH
END;
GO

-- Create procedure to verify loan balances
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'verify_loan_balances')
BEGIN
    DROP PROCEDURE [dbo].[verify_loan_balances]
END
GO

CREATE PROCEDURE [dbo].[verify_loan_balances]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Verify balances
    WITH BalanceCheck AS (
        SELECT 
            M_NO,
            LOAN_NO,
            [DATE],
            DEBIT,
            PRINCIPLE,
            INTEREST,
            TOTAL,
            BALANCE,
            LAG(BALANCE) OVER (PARTITION BY M_NO, LOAN_NO ORDER BY [DATE], 
                CASE WHEN OPERATOR = 'CWO' THEN 0 ELSE 1 END,
                CASE WHEN (PRINCIPLE IS NOT NULL OR INTEREST IS NOT NULL) THEN 0 ELSE 1 END) as prev_balance,
            ISNULL(PRINCIPLE, 0) + ISNULL(INTEREST, 0) - ISNULL(DEBIT, 0) as expected_change
        FROM [dbo].[MEMBER_LOANS]
    )
    SELECT 
        'Balance verification complete' as step,
        COUNT(*) as total_records,
        COUNT(DISTINCT M_NO) as total_accounts,
        COUNT(DISTINCT LOAN_NO) as total_loans,
        SUM(CASE WHEN OPERATOR = 'CWO' THEN 1 ELSE 0 END) as total_cwo_records,
        SUM(CASE WHEN PRINCIPLE IS NOT NULL OR INTEREST IS NOT NULL THEN 1 ELSE 0 END) as total_credit_transactions,
        SUM(CASE WHEN DEBIT IS NOT NULL THEN 1 ELSE 0 END) as total_debit_transactions,
        SUM(ISNULL(PRINCIPLE, 0)) as total_principles,
        SUM(ISNULL(INTEREST, 0)) as total_interests,
        SUM(ISNULL(DEBIT, 0)) as total_debits,
        SUM(ISNULL(PRINCIPLE, 0)) + SUM(ISNULL(INTEREST, 0)) - SUM(ISNULL(DEBIT, 0)) as net_balance
    FROM [dbo].[MEMBER_LOANS]
    WHERE CONCAT(M_NO, '-', LOAN_NO) IN (
        SELECT CONCAT(M_NO, '-', LOAN_NO)
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
WHERE object_id = OBJECT_ID('[dbo].[MEMBER_LOANS]')
ORDER BY c.column_id;
