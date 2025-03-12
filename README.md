# Balance Calculator SQL Guide

This guide explains how to use the balance calculator SQL script with your existing database that contains transaction data needing balance calculations.

## Overview

The balance calculator is designed to:
1. Calculate running balances for transactions
2. Handle multiple accounts simultaneously
3. Process transactions in the correct order based on:
   - Date (ascending)
   - CWO transactions first
   - Credits before debits
   - Original transaction order

## Prerequisites

Your database should have a transactions table with the following columns:
```sql
CREATE TABLE transactions (
    E_NO VARCHAR(10),      -- Employee/Account number
    M_NO VARCHAR(10),      -- Member/Sub-account number
    [DATE] DATE,          -- Transaction date
    DEBIT DECIMAL(10,2),  -- Debit amount (positive for additions)
    CREDIT DECIMAL(10,2), -- Credit amount (positive for additions)
    BALANCE DECIMAL(10,2), -- Running balance (will be calculated)
    OPERATOR VARCHAR(10)   -- Transaction operator (CWO for opening balance)
)
```

## Installation Steps

1. **Backup Your Data**
   ```sql
   -- Create a backup of your transactions table
   SELECT * INTO transactions_backup FROM transactions;
   ```

2. **Add Required Columns** (if missing)
   ```sql
   -- Add BALANCE column if it doesn't exist
   IF NOT EXISTS (SELECT * FROM sys.columns 
                 WHERE object_id = OBJECT_ID('transactions') 
                 AND name = 'BALANCE')
   BEGIN
       ALTER TABLE transactions ADD BALANCE DECIMAL(10,2);
   END

   -- Add OPERATOR column if it doesn't exist
   IF NOT EXISTS (SELECT * FROM sys.columns 
                 WHERE object_id = OBJECT_ID('transactions') 
                 AND name = 'OPERATOR')
   BEGIN
       ALTER TABLE transactions ADD OPERATOR VARCHAR(10);
   END
   ```

3. **Install the Balance Calculator Procedure**
   ```sql
   CREATE PROCEDURE [dbo].[update_running_balances]
   AS
   BEGIN
       SET NOCOUNT ON;
       
       -- Create temporary table for ordered transactions
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
       
       DECLARE account_cursor CURSOR FOR 
           SELECT DISTINCT e_no, m_no 
           FROM #temp_ordered_transactions 
           ORDER BY e_no, m_no;

       OPEN account_cursor
       
       FETCH NEXT FROM account_cursor INTO @current_e_no, @current_m_no
       
       WHILE @@FETCH_STATUS = 0
       BEGIN
           SET @running_balance = 0
           
           UPDATE t
           SET @running_balance = t.balance = @running_balance + t.credit + t.debit
           FROM transactions t
           INNER JOIN #temp_ordered_transactions tot
           ON t.e_no = tot.e_no
           AND t.m_no = tot.m_no
           AND t.[date] = tot.[date]
           AND ISNULL(t.credit, 0) = ISNULL(tot.credit, 0)
           AND ISNULL(t.debit, 0) = ISNULL(tot.debit, 0)
           WHERE tot.e_no = @current_e_no
           AND tot.m_no = @current_m_no
           ORDER BY tot.row_seq;
           
           FETCH NEXT FROM account_cursor INTO @current_e_no, @current_m_no
       END
       
       CLOSE account_cursor
       DEALLOCATE account_cursor
   END
   ```

## Usage

1. **Mark Opening Balances**
   ```sql
   -- Mark the first transaction for each account as CWO
   UPDATE t
   SET OPERATOR = 'CWO'
   FROM transactions t
   INNER JOIN (
       SELECT e_no, m_no, MIN([date]) as first_date
       FROM transactions
       GROUP BY e_no, m_no
   ) f ON t.e_no = f.e_no 
       AND t.m_no = f.m_no 
       AND t.[date] = f.first_date;
   ```

2. **Calculate Balances**
   ```sql
   -- Run the balance calculator
   EXEC [dbo].[update_running_balances];
   ```

3. **Verify Results**
   ```sql
   -- View updated balances
   SELECT 
       e_no,
       m_no,
       [date],
       debit,
       credit,
       balance,
       operator
   FROM transactions
   ORDER BY e_no, m_no, [date];
   ```

## Troubleshooting

1. **Verify Opening Balances**
   ```sql
   -- Check if all accounts have CWO transactions
   SELECT e_no, m_no, MIN([date]) as first_date, 
          MAX(CASE WHEN operator = 'CWO' THEN 1 ELSE 0 END) as has_cwo
   FROM transactions
   GROUP BY e_no, m_no
   HAVING MAX(CASE WHEN operator = 'CWO' THEN 1 ELSE 0 END) = 0;
   ```

2. **Check for Data Issues**
   ```sql
   -- Check for NULL amounts
   SELECT *
   FROM transactions
   WHERE (debit IS NULL AND credit IS NULL)
   OR (debit < 0 OR credit < 0);
   ```

3. **Verify Balance Progression**
   ```sql
   -- Check for unexpected balance changes
   WITH BalanceChanges AS (
       SELECT 
           e_no,
           m_no,
           [date],
           debit,
           credit,
           balance,
           LAG(balance) OVER (PARTITION BY e_no, m_no ORDER BY [date]) as prev_balance,
           credit + debit as expected_change
       FROM transactions
   )
   SELECT *
   FROM BalanceChanges
   WHERE ABS((balance - ISNULL(prev_balance, 0)) - expected_change) > 0.01;
   ```

## Important Notes

1. **Backup Data**: Always backup your data before running balance updates
2. **Transaction Order**: 
   - CWO transactions are processed first for each date
   - Credits are processed before debits on the same date
   - Original transaction order is preserved within same type
3. **Performance**: 
   - For large datasets, consider running updates in batches by date range
   - Create indexes on (e_no, m_no, date) for better performance
4. **Data Integrity**:
   - Ensure no negative amounts in debit/credit columns
   - Each account should have one CWO transaction
   - Dates should be valid and not in the future

## Compatibility

- SQL Server 2014 and later versions
- Azure SQL Edge
- Azure SQL Database

## Support

For large datasets or specific scenarios, consider these modifications:

1. **Batch Processing**
   ```sql
   -- Process by date ranges
   DECLARE @start_date DATE = '2024-01-01';
   DECLARE @end_date DATE = '2024-01-31';
   
   -- Add WHERE clause to procedure
   WHERE [date] BETWEEN @start_date AND @end_date
   ```

2. **Performance Optimization**
   ```sql
   -- Add helpful indexes
   CREATE INDEX IX_transactions_account_date 
   ON transactions(e_no, m_no, [date]);
   ```

3. **Error Handling**
   ```sql
   -- Add TRY-CATCH block
   BEGIN TRY
       EXEC [dbo].[update_running_balances];
   END TRY
   BEGIN CATCH
       SELECT 
           ERROR_NUMBER() AS ErrorNumber,
           ERROR_MESSAGE() AS ErrorMessage;
   END CATCH
   ```

## Detailed Examples

### Example 1: Basic Usage with Sample Data
```sql
-- 1. First, let's insert some sample transactions
INSERT INTO transactions (E_NO, M_NO, [DATE], DEBIT, CREDIT, OPERATOR) VALUES 
('EMP1', '111', '2024-01-01', 0, 1000.00, 'CWO'),     -- Opening balance
('EMP1', '111', '2024-01-02', 0, 500.00, NULL),       -- Salary credit
('EMP1', '111', '2024-01-02', 200.00, 0, NULL),       -- Debit transaction
('EMP1', '111', '2024-01-03', 0, 300.00, NULL);       -- Another credit

-- 2. Run the balance calculator
EXEC [dbo].[update_running_balances];

-- 3. View the results
SELECT *, 
       LAG(balance) OVER (ORDER BY [date]) as previous_balance,
       balance - LAG(balance) OVER (ORDER BY [date]) as change
FROM transactions 
WHERE E_NO = 'EMP1' AND M_NO = '111'
ORDER BY [date];

-- Expected output:
-- DATE        DEBIT   CREDIT  BALANCE  OPERATOR  PREV_BAL  CHANGE
-- 2024-01-01  0       1000    1000     CWO       NULL      NULL
-- 2024-01-02  0       500     1500     NULL      1000      500
-- 2024-01-02  200     0       1700     NULL      1500      200
-- 2024-01-03  0       300     2000     NULL      1700      300
```

### Example 2: Handling Multiple Accounts
```sql
-- Insert transactions for multiple accounts
INSERT INTO transactions (E_NO, M_NO, [DATE], DEBIT, CREDIT, OPERATOR) VALUES 
-- Account 1
('EMP1', '111', '2024-01-01', 0, 1000.00, 'CWO'),
('EMP1', '111', '2024-01-02', 200.00, 0, NULL),
-- Account 2
('EMP2', '222', '2024-01-01', 0, 2000.00, 'CWO'),
('EMP2', '222', '2024-01-02', 0, 500.00, NULL),
-- Account 3 (same employee, different member number)
('EMP1', '333', '2024-01-01', 0, 3000.00, 'CWO'),
('EMP1', '333', '2024-01-02', 1000.00, 0, NULL);

-- Run calculator and view results by account
EXEC [dbo].[update_running_balances];

SELECT e_no, m_no, 
       MIN([date]) as first_date,
       MAX([date]) as last_date,
       MIN(balance) as min_balance,
       MAX(balance) as max_balance,
       COUNT(*) as transaction_count
FROM transactions
GROUP BY e_no, m_no
ORDER BY e_no, m_no;
```

### Example 3: Same-Day Transaction Ordering
```sql
-- Insert multiple transactions on the same day
INSERT INTO transactions (E_NO, M_NO, [DATE], DEBIT, CREDIT, OPERATOR) VALUES 
('EMP1', '111', '2024-01-01', 0, 1000.00, 'CWO'),     -- 1st: CWO
('EMP1', '111', '2024-01-01', 0, 500.00, NULL),       -- 2nd: Credit
('EMP1', '111', '2024-01-01', 200.00, 0, NULL),       -- 3rd: Debit
('EMP1', '111', '2024-01-01', 0, 300.00, NULL);       -- 4th: Credit

EXEC [dbo].[update_running_balances];

-- View the ordering
SELECT ROW_NUMBER() OVER (ORDER BY 
    [date],
    CASE WHEN OPERATOR = 'CWO' THEN 0 ELSE 1 END,
    CASE WHEN credit > 0 THEN 0 ELSE 1 END
) as process_order,
[date], debit, credit, balance, operator
FROM transactions
WHERE E_NO = 'EMP1' AND M_NO = '111'
AND [date] = '2024-01-01'
ORDER BY process_order;
```

## Advanced Optimization Tips

### 1. Index Optimization
```sql
-- Create covering index for common queries
CREATE INDEX IX_transactions_main ON transactions
(
    e_no, m_no, [date]
)
INCLUDE 
(
    debit, credit, balance, operator
);

-- Index for date range queries
CREATE INDEX IX_transactions_date ON transactions([date])
INCLUDE (e_no, m_no, debit, credit);

-- Drop unused indexes
DROP INDEX IF EXISTS IX_transactions_unused ON transactions;
```

### 2. Partitioning for Large Datasets
```sql
-- Create partition function
CREATE PARTITION FUNCTION PF_TransactionDate (date)
AS RANGE RIGHT FOR VALUES 
('2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01');

-- Create partition scheme
CREATE PARTITION SCHEME PS_TransactionDate
AS PARTITION PF_TransactionDate
ALL TO ([PRIMARY]);

-- Create partitioned table
CREATE TABLE transactions_partitioned
(
    E_NO VARCHAR(10),
    M_NO VARCHAR(10),
    [DATE] DATE,
    DEBIT DECIMAL(10,2),
    CREDIT DECIMAL(10,2),
    BALANCE DECIMAL(10,2),
    OPERATOR VARCHAR(10)
) ON PS_TransactionDate([DATE]);
```

### 3. Batch Processing Implementation
```sql
-- Process in batches of 100,000 records
CREATE PROCEDURE [dbo].[update_running_balances_batch]
    @batch_size INT = 100000
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @start_date DATE, @end_date DATE;
    DECLARE @processed INT = 0;
    
    -- Get date range
    SELECT @start_date = MIN([date]), @end_date = MAX([date])
    FROM transactions;
    
    WHILE @start_date <= @end_date
    BEGIN
        -- Process one day at a time
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
            WHERE [date] = @start_date
        ) sub ON t.e_no = sub.e_no 
              AND t.m_no = sub.m_no 
              AND t.[date] = sub.[date];
        
        SET @processed = @processed + @@ROWCOUNT;
        SET @start_date = DATEADD(day, 1, @start_date);
        
        -- Progress report
        RAISERROR ('Processed %d records through date %s', 0, 1, @processed, @start_date) WITH NOWAIT;
        
        -- Optional: Add delay to reduce resource impact
        WAITFOR DELAY '00:00:00.1';
    END;
END;
```

### 4. Monitoring and Maintenance
```sql
-- Monitor long-running executions
SELECT r.session_id,
       r.status,
       r.command,
       r.cpu_time,
       r.total_elapsed_time,
       t.text
FROM sys.dm_exec_requests r
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
WHERE r.command = 'UPDATE';

-- Update statistics
UPDATE STATISTICS transactions
WITH FULLSCAN;

-- Rebuild indexes if fragmented
ALTER INDEX ALL ON transactions REBUILD;
```

### 5. Memory Optimization
```sql
-- Create memory-optimized table
CREATE TABLE transactions_memory
(
    E_NO VARCHAR(10),
    M_NO VARCHAR(10),
    [DATE] DATE,
    DEBIT DECIMAL(10,2),
    CREDIT DECIMAL(10,2),
    BALANCE DECIMAL(10,2),
    OPERATOR VARCHAR(10),
    CONSTRAINT PK_transactions_memory PRIMARY KEY NONCLUSTERED (E_NO, M_NO, [DATE])
)
WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);

-- Memory-optimized procedure
CREATE PROCEDURE [dbo].[update_running_balances_memory]
WITH NATIVE_COMPILATION, SCHEMABINDING
AS
BEGIN ATOMIC
WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')
    
    -- Memory-optimized table operations here
    -- Note: Some T-SQL constructs are not supported in natively compiled procedures
END;
```

### 6. Error Prevention
```sql
-- Add constraints to prevent invalid data
ALTER TABLE transactions
ADD CONSTRAINT CK_transactions_amounts 
    CHECK (debit >= 0 AND credit >= 0);

ALTER TABLE transactions
ADD CONSTRAINT CK_transactions_date 
    CHECK ([date] <= GETDATE());

-- Add unique constraint for transaction uniqueness
ALTER TABLE transactions
ADD CONSTRAINT UQ_transactions_unique 
    UNIQUE (E_NO, M_NO, [DATE], DEBIT, CREDIT, OPERATOR);
``` 