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
    FROM {{ source('bronze','BRONZE_TRANSACTIONS_RAW') }}
),
-- Business Columns(parsed, json extraction, datatype conversion)
flattened AS (
    SELECT
        -- Business Columns
        payload:after:transaction_id::STRING                         AS transaction_id,
        TRY_TO_NUMBER(payload:after:account_id::STRING)              AS account_id,
        payload:after:transaction_type::STRING                       AS transaction_type,
        TRY_TO_DECIMAL(payload:after:amount::STRING,18,2)            AS amount,
        TRY_TO_NUMBER(payload:after:related_account_id::STRING)      AS related_account_id,
        payload:after:transaction_status::STRING                     AS transaction_status,
        TRY_TO_TIMESTAMP_NTZ(payload:after:transaction_date::STRING) AS transaction_date,
        -- CDC Metadata
        payload:op::STRING                                           AS cdc_operation,
        payload:source:snapshot::STRING                              AS snapshot_flag,
        TRY_TO_NUMBER(payload:source:lsn::STRING)                    AS source_lsn,
        TRY_TO_NUMBER(payload:source:txId::STRING)                   AS source_transaction_id,
        payload:source:table::STRING                                 AS source_table,
        payload:source:schema::STRING                                AS source_schema,
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
    WHERE transaction_id IS NOT NULL
)
SELECT
    transaction_id,
    account_id,
    transaction_type,
    amount,
    related_account_id,
    transaction_status,
    transaction_date,

    cdc_operation,
    snapshot_flag,
    source_lsn,
    source_transaction_id,
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