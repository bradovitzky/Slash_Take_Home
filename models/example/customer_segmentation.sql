-- Segment customers into types based on MCC or merchant description
-- TODO: Figure out how to segment based on confusing codes and descriptions

WITH customer_type AS (
    SELECT 
        entity_id,
        CASE 
            WHEN ????? THEN 'Ticket Broker'
            WHEN ????? THEN 'Media Buyer'
            ELSE 'Other'
        END AS customer_type
    FROM card_transactions
    left join entity on entity.subaccount_id = card_transactions.subaccount_id
    GROUP BY entity_id, customer_type
)
-- Calculate spend patterns by customer type
SELECT 
    customer_type,
    SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) AS total_spend,
    COUNT(DISTINCT entity_id) AS active_customers
FROM customer_type
JOIN card_transactions
    ON customer_type.entity_id = card_transactions.entity_id
GROUP BY customer_type
