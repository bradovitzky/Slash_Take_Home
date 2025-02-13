-- Calculate customer concentration at top merchants
WITH merchant_gmv AS (
    SELECT
        entity_id,
        SUM(CASE WHEN transaction_amount < 0 THEN transaction_amount ELSE 0 END) AS settled_amount
    FROM card_transactions
    left join entity on entity.subaccount_id = card_transactions.subaccount_id
    GROUP BY entity_id
),

total_gmv AS (
    SELECT
        SUM(CASE WHEN transaction_amount < 0 THEN transaction_amount ELSE 0 END) AS total_settled_amount
    FROM card_transactions
)

SELECT
    entity_id,
    settled_amount,
    (settled_amount / total_settled_amount) * 100 AS concentration_percentage
FROM merchant_gmv, total_gmv
ORDER BY settled_amount ASC
LIMIT 10