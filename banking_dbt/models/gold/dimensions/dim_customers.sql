{{ config(materialized='table', schema='GOLD') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['customer_id','dbt_valid_from']
    ) }} AS customer_sk,
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    created_at,
    updated_at,
    dbt_valid_from AS effective_from,
    dbt_valid_to AS effective_to,
    dbt_valid_to IS NULL AS is_current
FROM {{ ref('customers_snapshot') }}
--