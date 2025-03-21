-- First, create a temporary table with row numbers to ensure ordering
WITH OrderedData AS (
    SELECT 
        M_NO,
        LOAN_NO,
        [DATE],
        DEBIT,
        PRINCIPLE,
        INTEREST,
        ROW_NUMBER() OVER (PARTITION BY M_NO, LOAN_NO ORDER BY [DATE]) AS RowNum
    FROM YourTableName
),
-- Calculate running TOTAL and BALANCE
RunningTotals AS (
    SELECT 
        od.M_NO,
        od.LOAN_NO,
        od.[DATE],
        od.DEBIT,
        od.PRINCIPLE,
        od.INTEREST,
        od.RowNum,
        od.DEBIT + od.INTEREST - od.PRINCIPLE AS TOTAL,
        SUM(od.DEBIT + od.INTEREST - od.PRINCIPLE) OVER (
            PARTITION BY od.M_NO, od.LOAN_NO 
            ORDER BY od.RowNum
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS BALANCE
    FROM OrderedData od
)

-- Update the TOTAL column
UPDATE t
SET TOTAL = rt.TOTAL
FROM YourTableName t
JOIN RunningTotals rt ON t.M_NO = rt.M_NO AND t.LOAN_NO = rt.LOAN_NO AND t.[DATE] = rt.[DATE];

-- Update the BALANCE column
UPDATE t
SET BALANCE = rt.BALANCE
FROM YourTableName t
JOIN RunningTotals rt ON t.M_NO = rt.M_NO AND t.LOAN_NO = rt.LOAN_NO AND t.[DATE] = rt.[DATE];