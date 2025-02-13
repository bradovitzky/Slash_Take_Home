-- Top 10 merchants by GMV
WITH merchant_gmv AS (
    SELECT
        entity_id,
        SUM(CASE WHEN transaction_amount < 0 THEN transaction_amount ELSE 0 END) AS settled_amount
    FROM card_transactions
    left join entity on entity.subaccount_id = card_transactions.subaccount_id
    GROUP BY entity_id
)

SELECT
    *
FROM merchant_gmv
ORDER BY settled_amount ASC
LIMIT 10
