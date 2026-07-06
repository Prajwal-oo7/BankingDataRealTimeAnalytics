{{ config(materialized='table', schema='SILVER') }}


WITH bronze AS (
    SELECT
        ingestion_timestamp,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        kafka_key,
        kafka_timestamp,
        payload,
        source_file
    FROM {{ source('bronze','BRONZE_ACCOUNTS_RAW') }}
),
-- Business Columns(parsed, json extraction, datatype conversion)
flattened AS (
    SELECT
        -- Business Columns
        TRY_TO_NUMBER(payload:after:account_id::STRING)           AS account_id,
        TRY_TO_NUMBER(payload:after:customer_id::STRING)          AS customer_id,
        payload:after:account_type::STRING                        AS account_type,
        payload:after:account_status::STRING                      AS account_status,
        payload:after:currency::STRING                            AS currency,
        TRY_TO_DECIMAL(payload:after:balance::STRING,18,2)        AS balance,
        TRY_TO_TIMESTAMP_NTZ(payload:after:created_at::STRING)    AS created_at,
        TRY_TO_TIMESTAMP_NTZ(payload:after:updated_at::STRING)    AS updated_at,
        -- CDC Metadata
        payload:op::STRING                                        AS cdc_operation,
        payload:source:snapshot::STRING                           AS snapshot_flag,
        TRY_TO_NUMBER(payload:source:lsn::STRING)                 AS source_lsn,
        TRY_TO_NUMBER(payload:source:txId::STRING)                AS transaction_id,
        payload:source:table::STRING                              AS source_table,
        payload:source:schema::STRING                             AS source_schema,
        -- Kafka Metadata
        kafka_topic,
        kafka_partition,
        kafka_offset,
        kafka_key,
        kafka_timestamp,
        ingestion_timestamp,
        source_file
    FROM bronze
),
-- filtering, deduplication, business logic
final AS (
    SELECT *
    FROM flattened
    WHERE account_id IS NOT NULL
)
SELECT
    account_id,
    customer_id,
    account_type,
    account_status,
    currency,
    balance,
    created_at,
    updated_at,

    cdc_operation,
    snapshot_flag,
    source_lsn,
    transaction_id,
    source_table,
    source_schema,

    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_key,
    kafka_timestamp,
    ingestion_timestamp,
    source_file
FROM final