{{ config(materialized='table', schema='GOLD') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['account_id','dbt_valid_from']
    ) }} AS account_sk,
    account_id,
    customer_id,
    account_type,
    account_status,
    currency,
    balance,
    created_at,
    updated_at,
    dbt_valid_from AS effective_from,
    dbt_valid_to AS effective_to,
    dbt_valid_to IS NULL AS is_current
FROM {{ ref('accounts_snapshot') }}