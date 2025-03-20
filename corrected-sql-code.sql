-- Loan Balance Calculator for SQL Server 2014
-- Corrected version for loan accounts and principle/interest tracking

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
WHERE TABLE_NAME = 'PLOAN1';
GO

-- Create index for better performance
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_PLOAN1_Account_Loan_Date' AND object_id = OBJECT_ID('[dbo].[PLOAN1]'))
BEGIN
    CREATE INDEX IX_PLOAN1_Account_Loan_Date ON [dbo].[PLOAN1] (M_NO, LOAN_NO, [DATE]);
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

        -- Create a more reliable ordering system that properly handles same-day transactions
        -- First create a table with sequential IDs for tie-breaking
        IF OBJECT_ID('tempdb..#temp_ids') IS NOT NULL
            DROP TABLE #temp_ids;
            
        SELECT 
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS seq_id,
            M_NO,
            LOAN_NO,
            [DATE],
            PRINCIPLE,
            INTEREST,
            DEBIT,
            OPERATOR
        INTO #temp_ids
        FROM [dbo].[PLOAN1];
        
        -- Now create the ordered transactions using the sequential IDs
        SELECT 
            t.*,
            ROW_NUMBER() OVER (
                PARTITION BY t.M_NO, t.LOAN_NO
                ORDER BY 
                    t.[DATE], 
                    -- Make sure CWO (opening balance) comes first within a day
                    CASE WHEN t.OPERATOR = 'CWO' THEN 0 ELSE 1 END,
                    -- Then handle credits (principle/interest) before debits
                    CASE 
                        WHEN (t.PRINCIPLE IS NOT NULL OR t.INTEREST IS NOT NULL) AND t.DEBIT IS NULL THEN 1
                        WHEN t.DEBIT IS NOT NULL AND (t.PRINCIPLE IS NULL AND t.INTEREST IS NULL) THEN 2
                        ELSE 3 -- Both or neither
                    END,
                    -- Use the sequential ID for stable ordering
                    id.seq_id
            ) as row_seq
        INTO #temp_ordered_transactions
        FROM [dbo].[PLOAN1] t
        INNER JOIN #temp_ids id ON 
            t.M_NO = id.M_NO AND
            t.LOAN_NO = id.LOAN_NO AND
            t.[DATE] = id.[DATE] AND
            ISNULL(t.PRINCIPLE, 0) = ISNULL(id.PRINCIPLE, 0) AND
            ISNULL(t.INTEREST, 0) = ISNULL(id.INTEREST, 0) AND
            ISNULL(t.DEBIT, 0) = ISNULL(id.DEBIT, 0) AND
            ISNULL(t.OPERATOR, '') = ISNULL(id.OPERATOR, '');

        -- Add identity column to ensure unique matching during update
        ALTER TABLE #temp_ordered_transactions ADD tmp_id INT IDENTITY(1,1) PRIMARY KEY;

        -- Calculate running balances in a new temporary table
        IF OBJECT_ID('tempdb..#temp_balance_updates') IS NOT NULL
            DROP TABLE #temp_balance_updates;

        -- Use window functions for more reliable running balance calculation
        SELECT 
            t.*,
            SUM(ISNULL(t.PRINCIPLE, 0) + ISNULL(t.INTEREST, 0) - ISNULL(t.DEBIT, 0)) 
                OVER (PARTITION BY t.M_NO, t.LOAN_NO 
                      ORDER BY t.row_seq
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS calculated_balance,
            ISNULL(t.PRINCIPLE, 0) + ISNULL(t.INTEREST, 0) AS calculated_total
        INTO #temp_balance_updates
        FROM #temp_ordered_transactions t;

        -- Update the main table with calculated balances
        UPDATE ml
        SET 
            ml.BALANCE = tbu.calculated_balance,
            ml.TOTAL = tbu.calculated_total
        FROM [dbo].[PLOAN1] ml
        INNER JOIN #temp_balance_updates tbu ON 
            ml.M_NO = tbu.M_NO AND
            ml.LOAN_NO = tbu.LOAN_NO AND
            ml.[DATE] = tbu.[DATE] AND
            ISNULL(ml.PRINCIPLE, 0) = ISNULL(tbu.PRINCIPLE, 0) AND
            ISNULL(ml.INTEREST, 0) = ISNULL(tbu.INTEREST, 0) AND
            ISNULL(ml.DEBIT, 0) = ISNULL(tbu.DEBIT, 0) AND
            ISNULL(ml.OPERATOR, '') = ISNULL(tbu.OPERATOR, '');

        -- Get date range and summary statistics
        DECLARE @earliest_date DATE;
        DECLARE @latest_date DATE;
        DECLARE @total_principles DECIMAL(10,2);
        DECLARE @total_interests DECIMAL(10,2);
        DECLARE @total_debits DECIMAL(10,2);
        DECLARE @loans_processed INT;

        SELECT 
            @earliest_date = MIN([DATE]),
            @latest_date = MAX([DATE])
        FROM [dbo].[PLOAN1];

        SELECT
            @loans_processed = COUNT(DISTINCT CONCAT(M_NO, '-', LOAN_NO)),
            @total_principles = SUM(ISNULL(PRINCIPLE, 0)),
            @total_interests = SUM(ISNULL(INTEREST, 0)),
            @total_debits = SUM(ISNULL(DEBIT, 0))
        FROM [dbo].[PLOAN1];

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
        FROM [dbo].[PLOAN1];

    END TRY
    BEGIN CATCH
        SELECT 
            ERROR_NUMBER() AS ErrorNumber,
            ERROR_SEVERITY() AS ErrorSeverity,
            ERROR_STATE() AS ErrorState,
            ERROR_PROCEDURE() AS ErrorProcedure,
            ERROR_LINE() AS ErrorLine,
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
        -- Use a CTE to prevent race conditions
        WITH FirstTransactions AS (
            SELECT 
                t.M_NO, 
                t.LOAN_NO, 
                t.[DATE],
                ROW_NUMBER() OVER (PARTITION BY t.M_NO, t.LOAN_NO ORDER BY t.[DATE]) AS rn
            FROM [dbo].[PLOAN1] t
        )
        UPDATE ml
        SET OPERATOR = 'CWO'
        FROM [dbo].[PLOAN1] ml
        INNER JOIN FirstTransactions ft ON 
            ml.M_NO = ft.M_NO AND 
            ml.LOAN_NO = ft.LOAN_NO AND 
            ml.[DATE] = ft.[DATE] AND
            ft.rn = 1;

        -- Return summary
        SELECT 'Opening balances marked' as step,
               COUNT(*) as records_marked,
               COUNT(DISTINCT M_NO) as accounts_marked,
               COUNT(DISTINCT LOAN_NO) as loans_marked
        FROM [dbo].[PLOAN1]
        WHERE OPERATOR = 'CWO';
    END TRY
    BEGIN CATCH
        SELECT 
            ERROR_NUMBER() AS ErrorNumber,
            ERROR_SEVERITY() AS ErrorSeverity,
            ERROR_STATE() AS ErrorState,
            ERROR_PROCEDURE() AS ErrorProcedure,
            ERROR_LINE() AS ErrorLine,
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
    
    -- Create a temp table with the correct ordering for accurate balance verification
    IF OBJECT_ID('tempdb..#verification_ids') IS NOT NULL
        DROP TABLE #verification_ids;
        
    -- First create sequential IDs for stable ordering
    SELECT 
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS seq_id,
        M_NO,
        LOAN_NO,
        [DATE],
        PRINCIPLE,
        INTEREST,
        DEBIT,
        OPERATOR,
        BALANCE,
        TOTAL
    INTO #verification_ids
    FROM [dbo].[PLOAN1];
    
    IF OBJECT_ID('tempdb..#verification_check') IS NOT NULL
        DROP TABLE #verification_check;
        
    -- Now create the verification table with proper ordering
    SELECT 
        v.M_NO,
        v.LOAN_NO,
        v.[DATE],
        v.DEBIT,
        v.PRINCIPLE,
        v.INTEREST,
        v.TOTAL,
        v.BALANCE,
        v.OPERATOR,
        ROW_NUMBER() OVER (
            PARTITION BY v.M_NO, v.LOAN_NO
            ORDER BY 
                v.[DATE], 
                CASE WHEN v.OPERATOR = 'CWO' THEN 0 ELSE 1 END,
                CASE 
                    WHEN (v.PRINCIPLE IS NOT NULL OR v.INTEREST IS NOT NULL) AND v.DEBIT IS NULL THEN 1
                    WHEN v.DEBIT IS NOT NULL AND (v.PRINCIPLE IS NULL AND v.INTEREST IS NULL) THEN 2
                    ELSE 3
                END,
                id.seq_id
        ) as row_seq
    INTO #verification_check
    FROM [dbo].[PLOAN1] v
    INNER JOIN #verification_ids id ON 
        v.M_NO = id.M_NO AND
        v.LOAN_NO = id.LOAN_NO AND
        v.[DATE] = id.[DATE] AND
        ISNULL(v.PRINCIPLE, 0) = ISNULL(id.PRINCIPLE, 0) AND
        ISNULL(v.INTEREST, 0) = ISNULL(id.INTEREST, 0) AND
        ISNULL(v.DEBIT, 0) = ISNULL(id.DEBIT, 0) AND
        ISNULL(v.OPERATOR, '') = ISNULL(id.OPERATOR, '');
    
    -- Find loans with balance discrepancies
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
            row_seq,
            LAG(BALANCE) OVER (PARTITION BY M_NO, LOAN_NO ORDER BY row_seq) as prev_balance,
            ISNULL(PRINCIPLE, 0) + ISNULL(INTEREST, 0) - ISNULL(DEBIT, 0) as expected_change
        FROM #verification_check
    ),
    Discrepancies AS (
        SELECT
            M_NO,
            LOAN_NO,
            [DATE],
            BALANCE,
            prev_balance,
            expected_change,
            CASE
                WHEN prev_balance IS NULL THEN 0 -- First record
                ELSE ABS((BALANCE - prev_balance) - expected_change)
            END as discrepancy_amount
        FROM BalanceCheck
        WHERE 
            -- Only consider discrepancies above rounding error threshold
            CASE
                WHEN prev_balance IS NULL THEN 0
                ELSE ABS((BALANCE - prev_balance) - expected_change)
            END > 0.01
    )
    SELECT 
        'Balance verification complete' as step,
        (SELECT COUNT(*) FROM [dbo].[PLOAN1]) as total_records,
        (SELECT COUNT(DISTINCT M_NO) FROM [dbo].[PLOAN1]) as total_accounts,
        (SELECT COUNT(DISTINCT LOAN_NO) FROM [dbo].[PLOAN1]) as total_loans,
        (SELECT COUNT(*) FROM [dbo].[PLOAN1] WHERE OPERATOR = 'CWO') as total_cwo_records,
        (SELECT COUNT(*) FROM [dbo].[PLOAN1] WHERE PRINCIPLE IS NOT NULL OR INTEREST IS NOT NULL) as total_credit_transactions,
        (SELECT COUNT(*) FROM [dbo].[PLOAN1] WHERE DEBIT IS NOT NULL) as total_debit_transactions,
        (SELECT SUM(ISNULL(PRINCIPLE, 0)) FROM [dbo].[PLOAN1]) as total_principles,
        (SELECT SUM(ISNULL(INTEREST, 0)) FROM [dbo].[PLOAN1]) as total_interests,
        (SELECT SUM(ISNULL(DEBIT, 0)) FROM [dbo].[PLOAN1]) as total_debits,
        (SELECT COUNT(DISTINCT CONCAT(M_NO, '-', LOAN_NO)) FROM Discrepancies) as loans_with_discrepancies,
        (SELECT COUNT(*) FROM Discrepancies) as total_discrepancies,
        (SELECT MAX(discrepancy_amount) FROM Discrepancies) as max_discrepancy_amount;
        
    -- Return specific discrepancies (limited to top 100)
    SELECT TOP 100
        M_NO,
        LOAN_NO,
        [DATE],
        BALANCE,
        prev_balance,
        expected_change,
        discrepancy_amount,
        'Current balance should be ' + CAST(prev_balance + expected_change AS VARCHAR(20)) as correct_balance
    FROM Discrepancies
    ORDER BY discrepancy_amount DESC;
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
WHERE object_id = OBJECT_ID('[dbo].[PLOAN1]')
ORDER BY c.column_id;
