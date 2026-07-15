{{ config(materialized='table',schema='GOLD') }}


WITH txn AS (
    SELECT
        account_id,
        COUNT(*) AS total_transactions,
        SUM(amount) AS total_amount,
        AVG(amount) AS avg_transaction,
        SUM(
            CASE
                WHEN transaction_type = 'DEPOSIT' THEN amount
                ELSE 0
            END
        ) as deposits,
        SUM(
            CASE
                WHEN transaction_type = 'WITHDRAWAL' THEN amount
                ELSE 0
            END
        ) as withdrawals,

        MAX(transaction_date) as last_transaction
    FROM {{ ref('fact_transactions') }}
    GROUP BY account_id
)
SELECT
    a.account_sk,
    a.account_id,
    a.customer_id,
    a.account_type,
    a.account_status,
    a.currency,
    a.balance,
    a.created_at,
    COALESCE(t.total_transactions, 0) as total_transactions,
    COALESCE(t.total_amount, 0) as total_amount,
    COALESCE(t.avg_transaction, 0) as avg_transaction,
    COALESCE(t.deposits, 0) as deposits,
    COALESCE(t.withdrawals, 0) as withdrawals,
    t.last_transaction
FROM {{ ref('dim_accounts') }} as a
LEFT JOIN txn as t
    ON a.account_id = t.account_id
WHERE a.is_current
--