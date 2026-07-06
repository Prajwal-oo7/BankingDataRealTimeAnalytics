{{ config(materialized='table', schema='GOLD') }}


WITH account_summary AS (
    SELECT
        customer_id,
        COUNT(account_id)              AS total_accounts,
        SUM(balance)                   AS current_balance
    FROM {{ ref('dim_accounts') }}
    WHERE is_current
    GROUP BY customer_id
),

transaction_summary AS (
    SELECT
        customer_id,
        COUNT(transaction_id)          AS total_transactions,
        SUM(amount)                    AS total_amount,
        SUM(
            CASE
                WHEN transaction_type='DEPOSIT'
                THEN amount
                ELSE 0
            END
        ) AS total_deposits,

        SUM(
            CASE
                WHEN transaction_type='WITHDRAWAL'
                THEN amount
                ELSE 0
            END
        ) AS total_withdrawals,

        MAX(transaction_date) AS last_transaction

    FROM {{ ref('fact_transactions') }}
    GROUP BY customer_id
)

SELECT
    c.customer_sk,
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.created_at,
    COALESCE(a.total_accounts,0)      AS total_accounts,
    COALESCE(a.current_balance,0)     AS current_total_balance,
    COALESCE(t.total_transactions,0)  AS total_transactions,
    COALESCE(t.total_amount,0)        AS lifetime_transaction_amount,
    COALESCE(t.total_deposits,0)      AS total_deposits,
    COALESCE(t.total_withdrawals,0)   AS total_withdrawals,
    t.last_transaction
FROM {{ ref('dim_customers') }} c
LEFT JOIN account_summary a
    ON c.customer_id=a.customer_id
LEFT JOIN transaction_summary t
    ON c.customer_id=t.customer_id
WHERE c.is_current