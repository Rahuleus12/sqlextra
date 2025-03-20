USE cwizbank_adani;
GO

-- Create an index for performance optimization
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_PLOAN1_Account_Loan_Date' AND object_id = OBJECT_ID('[dbo].[PLOAN1]'))
BEGIN
    CREATE INDEX IX_PLOAN1_Account_Loan_Date ON [dbo].[PLOAN1] (M_NO, LOAN_NO, [DATE]);
END
GO

-- Drop the stored procedure if it already exists
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'update_ploan1_balances')
BEGIN
    DROP PROCEDURE [dbo].[update_ploan1_balances];
END
GO

-- Create procedure to update balances
CREATE PROCEDURE [dbo].[update_ploan1_balances]
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        -- Step 1: Create temporary table with ordered transactions
        IF OBJECT_ID('tempdb..#OrderedTransactions') IS NOT NULL
            DROP TABLE #OrderedTransactions;

        SELECT 
            M_NO, LOAN_NO, [DATE], DEBIT, PRINCIPLE, INTEREST, 
            (ISNULL(PRINCIPLE, 0) + ISNULL(INTEREST, 0)) AS TOTAL, BALANCE,
            ROW_NUMBER() OVER (
                PARTITION BY M_NO, LOAN_NO ORDER BY [DATE]
            ) AS row_seq
        INTO #OrderedTransactions
        FROM dbo.PLOAN1;

        -- Step 2: Compute running balance using window function
        WITH RunningBalance AS (
            SELECT 
                M_NO, LOAN_NO, [DATE], DEBIT, PRINCIPLE, INTEREST, TOTAL,
                SUM(TOTAL - DEBIT) OVER (
                    PARTITION BY M_NO, LOAN_NO ORDER BY row_seq 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS BALANCE
            FROM #OrderedTransactions
        )
        -- Step 3: Update the original table with new balances
        UPDATE dbo.PLOAN1
        SET BALANCE = RB.BALANCE
        FROM dbo.PLOAN1 P
        JOIN RunningBalance RB
            ON P.M_NO = RB.M_NO
            AND P.LOAN_NO = RB.LOAN_NO
            AND P.[DATE] = RB.[DATE];

        -- Step 4: Return summary of the update
        SELECT 
            'Balance calculation complete' AS step,
            COUNT(*) AS total_records_updated,
            COUNT(DISTINCT M_NO) AS total_members_processed,
            COUNT(DISTINCT LOAN_NO) AS total_loans_processed
        FROM dbo.PLOAN1;

    END TRY
    BEGIN CATCH
        SELECT 
            ERROR_NUMBER() AS ErrorNumber,
            ERROR_MESSAGE() AS ErrorMessage;
    END CATCH
END;
GO

-- Execute the procedure to update balances
EXEC dbo.update_ploan1_balances;
