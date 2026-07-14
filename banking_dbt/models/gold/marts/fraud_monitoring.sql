{{ config(materialized='table',schema='GOLD') }}


WITH hourly_activity AS (
    SELECT
        account_id,
        DATE_TRUNC('hour', transaction_date) AS transaction_hour,
        COUNT(*) AS transactions_per_hour
    FROM {{ ref('fact_transactions') }}
    GROUP BY
        account_id,
        DATE_TRUNC('hour', transaction_date)
)

SELECT
    f.transaction_sk,
    f.transaction_id,
    f.account_id,
    f.customer_id,
    f.amount,
    f.transaction_type,
    f.transaction_status,
    f.transaction_date,
    h.transactions_per_hour,
    CASE
        WHEN f.amount >= 10000 THEN 'HIGH_VALUE'
        WHEN h.transactions_per_hour >= 10 THEN 'HIGH_FREQUENCY'
        ELSE 'NORMAL'
    END AS fraud_flag
FROM {{ ref('fact_transactions') }} as f
LEFT JOIN hourly_activity as h
    ON
        f.account_id = h.account_id
        AND DATE_TRUNC('hour', f.transaction_date) = h.transaction_hour
--