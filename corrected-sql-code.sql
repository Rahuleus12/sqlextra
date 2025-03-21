-- Step 1: Create a temporary table to store ordered data with calculated TOTAL values
CREATE TABLE #TempOrderedData (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    M_NO INT,
    LOAN_NO INT,
    [DATE] DATETIME,  -- Assuming DATE is a datetime column, adjust if different
    DEBIT DECIMAL(18,2),
    PRINCIPLE DECIMAL(18,2),
    INTEREST DECIMAL(18,2),
    TOTAL DECIMAL(18,2),
    SeqNum INT
);

-- Step 2: Insert data with calculated TOTAL for each row and establish proper sequence
INSERT INTO #TempOrderedData (M_NO, LOAN_NO, [DATE], DEBIT, PRINCIPLE, INTEREST, TOTAL, SeqNum)
SELECT 
    M_NO, 
    LOAN_NO, 
    [DATE], 
    DEBIT, 
    PRINCIPLE, 
    INTEREST,
    DEBIT + INTEREST - PRINCIPLE AS TOTAL,
    ROW_NUMBER() OVER (PARTITION BY M_NO, LOAN_NO ORDER BY [DATE]) AS SeqNum
FROM YourTableName;

-- Step 3: Create another temp table with calculated running balances
CREATE TABLE #FinalData (
    ID INT,
    M_NO INT,
    LOAN_NO INT,
    [DATE] DATETIME,
    DEBIT DECIMAL(18,2),
    PRINCIPLE DECIMAL(18,2),
    INTEREST DECIMAL(18,2),
    TOTAL DECIMAL(18,2),
    BALANCE DECIMAL(18,2)
);

-- Step 4: Insert data with calculated BALANCE based on proper sequence
INSERT INTO #FinalData
SELECT 
    t.ID,
    t.M_NO,
    t.LOAN_NO,
    t.[DATE],
    t.DEBIT,
    t.PRINCIPLE,
    t.INTEREST,
    t.TOTAL,
    SUM(t.TOTAL) OVER (
        PARTITION BY t.M_NO, t.LOAN_NO 
        ORDER BY t.SeqNum
        ROWS UNBOUNDED PRECEDING
    ) AS BALANCE
FROM #TempOrderedData t;

-- Step 5: Update the original table with calculated values
UPDATE orig
SET 
    TOTAL = fd.TOTAL,
    BALANCE = fd.BALANCE
FROM YourTableName orig
JOIN #FinalData fd ON 
    orig.M_NO = fd.M_NO AND 
    orig.LOAN_NO = fd.LOAN_NO AND 
    orig.[DATE] = fd.[DATE];

-- Step 6: Clean up temp tables
DROP TABLE #TempOrderedData;
DROP TABLE #FinalData;