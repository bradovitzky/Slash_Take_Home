-- Calculate average spend per active card
WITH latest_card_status AS (
    SELECT 
        card_id,
        card_status,
        ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY timestamp DESC) AS row_num
    FROM card_events
),
active_cards AS (
    SELECT 
        card_id
    FROM latest_card_status
    WHERE row_num = 1 AND card_status = 'active'
)
SELECT 
    t.card_id,
    SUM(t.transaction_amount) as total_spend,
    COUNT(*) as total_transaction_count,
    total_spend/total_transaction_count as average_transaction_size
FROM active_cards ac
JOIN card_transactions t ON t.card_id = ac.card_id
WHERE t.transaction_amount < 0
GROUP BY t.card_id
ORDER BY card_id, total_spend DESC

