{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge',
    schema='GOLD'
) }}


SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['transaction_id']
    ) }} AS transaction_sk,
    a.account_sk,
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.transaction_type,
    t.transaction_status,
    CAST(t.amount AS NUMBER(12,2)) AS amount,
    t.related_account_id,   
    t.transaction_date,
    a.account_type,
    a.currency, 
    t.ingestion_timestamp,    
    CURRENT_TIMESTAMP() AS gold_load_timestamp
FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('dim_accounts') }} a
    ON t.account_id = a.account_id
    AND a.is_current = TRUE

WHERE ( t.cdc_operation != 'd' OR t.cdc_operation IS NULL )

{% if is_incremental() %}
    
    AND t.ingestion_timestamp > (SELECT COALESCE(MAX(ingestion_timestamp),'1900-01-01') FROM {{ this }})

{% endif %}