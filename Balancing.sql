SELECT 
    transaction_date,
    debit,
    credit,
    SUM(credit - debit) OVER (ORDER BY transaction_date 
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS predicted_balance
FROM predicting_table
ORDER BY transaction_date;
