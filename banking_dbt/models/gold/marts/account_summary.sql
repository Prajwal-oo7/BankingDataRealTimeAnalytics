{{ config(materialized='table',schema='GOLD') }}


WITH txn AS (
SELECT
    account_id,
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_transaction,
    SUM(
        CASE
            WHEN transaction_type='DEPOSIT'
            THEN amount
            ELSE 0
        END
    ) deposits,

    SUM(
        CASE
            WHEN transaction_type='WITHDRAWAL'
            THEN amount
            ELSE 0
        END
    ) withdrawals,

    MAX(transaction_date) last_transaction
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
    COALESCE(t.total_transactions,0) total_transactions,
    COALESCE(t.total_amount,0) total_amount,
    COALESCE(t.avg_transaction,0) avg_transaction,
    COALESCE(t.deposits,0) deposits,
    COALESCE(t.withdrawals,0) withdrawals,
    t.last_transaction
FROM {{ ref('dim_accounts') }} a
LEFT JOIN txn t
    ON a.account_id=t.account_id
WHERE a.is_current