-- Classify cards based on their most recent card_status
WITH latest_card_status_table AS (
    SELECT 
        card_id,
        card_status,
        ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY timestamp DESC) AS row_num
    FROM card_events
)

SELECT 
    CASE 
        WHEN card_status ='active' THEN 'Active'
        Else 'Inactive'
    END AS latest_card_status,
    COUNT(card_id) AS card_count
FROM latest_card_status_table
WHERE row_num = 1
GROUP BY latest_card_status
