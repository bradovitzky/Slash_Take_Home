WITH latest_card_status AS (
    SELECT 
        e.card_id,
        t.subaccount_id,
        e.card_status,
        ROW_NUMBER() OVER (PARTITION BY e.card_id ORDER BY e.timestamp DESC) AS row_num
    FROM card_events e
    LEFT JOIN card_transactions t ON t.card_id = e.card_id -- just join a transaction to get the subaccount for later usage
),
card_status_summary AS (
    SELECT
        card_id,
        subaccount_id,
        CASE 
            WHEN card_status IN ('deleted', 'paused') THEN 1
            ELSE 0
        END AS is_deleted_or_paused,
        CASE 
            WHEN card_status = 'active' THEN 1
            ELSE 0
        END AS is_active
    FROM latest_card_status
),
entity_cards AS (
    SELECT 
        e.entity_id,
        cs.card_id,
        cs.is_deleted_or_paused,
        cs.is_active
    FROM card_status_summary cs
    LEFT JOIN entity e on e.subaccount_id = cs.subaccount_id
),
card_transactions_summary AS (
    SELECT 
        t.card_id,
        SUM(t.transaction_amount) AS total_spend,
        COUNT(*) AS total_transaction_count
    FROM card_transactions t
    WHERE t.transaction_amount < 0
    GROUP BY t.card_id
)
SELECT 
    ec.entity_id,
    COUNT(DISTINCT ec.card_id) AS total_cards_created,  -- Total unique cards created per entity
    SUM(ec.is_deleted_or_paused) AS cards_deleted_or_paused,  -- Count of deleted/paused cards
    -- Calculate overall card utilization by dividing the number of active cards by total cards created
    (COUNT(DISTINCT CASE WHEN ec.is_active = 1 THEN ec.card_id END) * 1.0 / COUNT(DISTINCT ec.card_id)) AS overall_card_utilization,
    SUM(cts.total_spend) AS total_spend,  -- Total spend for all cards in the entity
    SUM(cts.total_spend) / COUNT(DISTINCT ec.card_id) AS avg_spend_per_card  -- Average spend per card
FROM entity_cards ec
LEFT JOIN card_transactions_summary cts ON ec.card_id = cts.card_id
GROUP BY ec.entity_id
ORDER BY total_spend DESC
