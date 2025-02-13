WITH monthly_gmv AS (
    SELECT 
        entity_id,
        strftime('%Y-%m', timestamp) AS month,
        SUM(CASE WHEN transaction_amount < 0 THEN transaction_amount ELSE 0 END) AS settled_amount,
        SUM(CASE WHEN transaction_amount > 0 THEN transaction_amount ELSE 0 END) AS refunded_amount 
    FROM card_transactions
    LEFT JOIN entity ON entity.subaccount_id = card_transactions.subaccount_id
    GROUP BY entity_id, month
),

-- Get the first transaction month for each entity
first_transaction_month AS (
    SELECT
        entity_id,
        MIN(strftime('%Y-%m', timestamp)) AS first_transaction_month
    FROM card_transactions
    LEFT JOIN entity ON entity.subaccount_id = card_transactions.subaccount_id
    GROUP BY entity_id
),

-- Get the GMV for each cohort and each month, and calculate retention
cohort_analysis AS (
    SELECT 
        f.first_transaction_month AS cohort_month,
        m.month,
        SUM(m.settled_amount) - SUM(m.refunded_amount) AS cohort_gmv  -- GMV for each cohort in each month
    FROM monthly_gmv m
    JOIN first_transaction_month f
        ON m.entity_id = f.entity_id
    GROUP BY f.first_transaction_month, m.month
),

-- Calculate the retention rate based on the initial GMV for each cohort
cohort_initial_gmv AS (
    SELECT 
        cohort_month,
        SUM(cohort_gmv) AS initial_gmv
    FROM cohort_analysis
    WHERE month = cohort_month
    GROUP BY cohort_month
)

-- Final selection of cohort analysis with retention rates
SELECT 
    a.cohort_month,
    a.month,
    a.cohort_gmv,
    (a.cohort_gmv / i.initial_gmv) * 100 AS retention_rate  -- Retention rate is calculated as GMV in month / initial GMV
FROM cohort_analysis a
JOIN cohort_initial_gmv i
    ON a.cohort_month = i.cohort_month
ORDER BY a.cohort_month, a.month
