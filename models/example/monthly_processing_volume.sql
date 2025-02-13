-- Calculate GMV
WITH monthly_transactions AS (
    SELECT 
        entity_id,
        date_trunc('month', timestamp) as month,  -- Group by year and month
        SUM(CASE WHEN transaction_amount < 0 THEN transaction_amount ELSE 0 END) AS settled_amount,
        SUM(CASE WHEN transaction_amount > 0 THEN transaction_amount ELSE 0 END) AS refunded_amount
    FROM card_transactions
    left join entity on entity.subaccount_id = card_transactions.subaccount_id
    GROUP BY entity_id, month
)

SELECT
    month,
    SUM(settled_amount) - SUM(refunded_amount) AS monthly_gmv
FROM monthly_transactions
GROUP BY month
ORDER BY month