-- Production Balance Calculator for SQL Server 2014
-- This script will work with an existing cwizbank_adani database and MEMBER1 table

USE cwizbank_adani;
GO

-- Set compatibility level to SQL Server 2014 if needed
IF (SELECT compatibility_level FROM sys.databases WHERE name = 'cwizbank_adani') <> 120
BEGIN
    ALTER DATABASE cwizbank_adani SET COMPATIBILITY_LEVEL = 120;
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
        BEGIN TRANSACTION;
        
        -- Check if temporary table exists and drop it
        IF OBJECT_ID('tempdb..#temp_ordered_transactions') IS NOT NULL
        BEGIN
            DROP TABLE #temp_ordered_transactions
        END
        
        -- Create temporary table with ordered transactions
        SELECT 
            t.*,
            ROW_NUMBER() OVER (
                PARTITION BY E_NO, M_NO 
                ORDER BY [DATE], 
                         CASE WHEN OPERATOR = 'CWO' THEN 0 ELSE 1 END,
                         CASE WHEN CREDIT IS NOT NULL THEN 0 ELSE 1 END
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

                SET @running_balance = @running_balance + @current_credit - @current_debit

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

        COMMIT TRANSACTION;
        
        -- Print summary of processed records
        SELECT 
            COUNT(*) as total_records_processed,
            COUNT(DISTINCT E_NO + M_NO) as total_accounts_processed,
            MIN([DATE]) as earliest_date,
            MAX([DATE]) as latest_date,
            SUM(ISNULL(CREDIT, 0)) as total_credits,
            SUM(ISNULL(DEBIT, 0)) as total_debits
        FROM MEMBER1;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
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
        BEGIN TRANSACTION;
        
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
        
        COMMIT TRANSACTION;
        
        -- Print summary of marked records
        SELECT 
            COUNT(*) as total_cwo_records,
            COUNT(DISTINCT E_NO + M_NO) as total_accounts_marked
        FROM MEMBER1
        WHERE OPERATOR = 'CWO';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
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
    
    -- Check for accounts missing CWO
    PRINT 'Checking for accounts missing CWO...';
    SELECT E_NO, M_NO,
           MAX(CASE WHEN OPERATOR = 'CWO' THEN 1 ELSE 0 END) as has_cwo
    FROM MEMBER1
    GROUP BY E_NO, M_NO
    HAVING MAX(CASE WHEN OPERATOR = 'CWO' THEN 1 ELSE 0 END) = 0;
    
    -- Check for invalid amounts
    PRINT 'Checking for invalid amounts...';
    SELECT E_NO, M_NO, [DATE], DEBIT, CREDIT, BALANCE, OPERATOR
    FROM MEMBER1
    WHERE (DEBIT IS NULL AND CREDIT IS NULL)
    OR (DEBIT < 0 OR CREDIT < 0);
    
    -- Verify balance progression
    PRINT 'Verifying balance progression...';
    WITH BalanceChanges AS (
        SELECT 
            E_NO,
            M_NO,
            [DATE],
            DEBIT,
            CREDIT,
            BALANCE,
            LAG(BALANCE) OVER (PARTITION BY E_NO, M_NO ORDER BY [DATE]) as prev_balance,
            CREDIT - DEBIT as expected_change
        FROM MEMBER1
    )
    SELECT E_NO, M_NO, [DATE], DEBIT, CREDIT, BALANCE, prev_balance, expected_change
    FROM BalanceChanges
    WHERE ABS((BALANCE - ISNULL(prev_balance, 0)) - expected_change) > 0.01;

    -- Print summary statistics
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT E_NO + M_NO) as total_accounts,
        SUM(CASE WHEN OPERATOR = 'CWO' THEN 1 ELSE 0 END) as total_cwo_records,
        SUM(CASE WHEN CREDIT IS NOT NULL THEN 1 ELSE 0 END) as total_credit_transactions,
        SUM(CASE WHEN DEBIT IS NOT NULL THEN 1 ELSE 0 END) as total_debit_transactions,
        SUM(ISNULL(CREDIT, 0)) as total_credits,
        SUM(ISNULL(DEBIT, 0)) as total_debits,
        SUM(ISNULL(CREDIT, 0) - ISNULL(DEBIT, 0)) as net_balance
    FROM MEMBER1;
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
WHERE object_id = OBJECT_ID('MEMBER1')
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