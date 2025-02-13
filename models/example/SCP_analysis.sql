-- Calculate SCP Enrollment Costs per entity based on card creation volumes
-- Start with cards created in any month
WITH monthly_card_creation AS (
    SELECT 
        entity_id,
        COUNT(card_events.card_id) AS cards_created,
        strftime('%Y-%m', card_events.timestamp) AS month
    FROM card_events
    left join card_transactions on card_events.card_id = card_transactions.card_id
    left join entity on entity.subaccount_id = card_transactions.subaccount_id
    WHERE event_type in ('create', 'pending creation')
    GROUP BY entity_id, month
),
-- Get enrollment costs for any entity on any month
scp_enrollment_costs AS (
    SELECT
        entity_id,
        month,
        CASE
            WHEN cards_created < 50000 THEN cards_created * 1.12
            WHEN cards_created BETWEEN 50000 AND 100000 THEN cards_created * 0.76
            WHEN cards_created BETWEEN 100000 AND 150000 THEN cards_created * 0.52
            ELSE cards_created * 0.34
        END AS enrollment_cost
    FROM monthly_card_creation
),
-- Calculate revenue benefits based on annual spend
annual_spend AS (
    SELECT 
        entity_id,
        SUM(CASE WHEN transaction_amount < 0 THEN transaction_amount ELSE 0 END) AS total_annual_spend
    FROM card_transactions
    left join entity on entity.subaccount_id = card_transactions.subaccount_id
    GROUP BY entity_id
),
scp_revenue_benefits AS (
    SELECT 
        entity_id,
        -1*total_annual_spend as positive_total_annual_spend,
        CASE 
            WHEN positive_total_annual_spend BETWEEN 20000 AND 40000 THEN 0.0015  -- Tier 1: 0.15% revenue
            WHEN positive_total_annual_spend BETWEEN 40000 AND 100000 THEN 0.0020  -- Tier 2: 0.20% revenue
            WHEN positive_total_annual_spend BETWEEN 100000 AND 250000 THEN 0.0030  -- Tier 3: 0.30% revenue
            WHEN positive_total_annual_spend > 250000 THEN 0.0035  -- Tier 4: 0.35% revenue
            ELSE 0
        END AS revenue_percentage
    FROM annual_spend
)
-- Now put it all together
SELECT
    s.entity_id,
    s.month,
    s.enrollment_cost,
    r.positive_total_annual_spend,
    r.revenue_percentage,
    r.positive_total_annual_spend * r.revenue_percentage AS potential_revenue_gain
FROM scp_enrollment_costs s
JOIN scp_revenue_benefits r
    ON s.entity_id = r.entity_id
ORDER BY potential_revenue_gain DESC
