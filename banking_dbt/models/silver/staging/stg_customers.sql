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
    FROM {{ source('bronze','BRONZE_CUSTOMERS_RAW') }}
),
-- Business Columns(parsed, json extraction, datatype conversion)
flattened AS (
    SELECT
        TRY_TO_NUMBER(payload:after:customer_id::STRING)      AS customer_id,
        payload:after:first_name::STRING             AS first_name,
        payload:after:last_name::STRING              AS last_name,
        payload:after:email::STRING                  AS email,
        payload:after:phone::STRING                  AS phone,
        TRY_TO_TIMESTAMP_NTZ(payload:after:created_at::STRING)      AS created_at,
        TRY_TO_TIMESTAMP_NTZ(payload:after:updated_at::STRING)      AS updated_at,
        -- CDC Metadata
        payload:op::STRING                           AS cdc_operation,
        payload:source:snapshot::STRING              AS snapshot_flag,
        TRY_TO_NUMBER(payload:source:lsn::STRING)    AS source_lsn,
        TRY_TO_NUMBER(payload:source:txId::STRING)   AS transaction_id,
        payload:source:table::STRING                 AS source_table,
        payload:source:schema::STRING                AS source_schema,
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
    SELECT * FROM flattened
    WHERE customer_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY customer_id
        ORDER BY
            updated_at DESC,
            source_lsn DESC,
            kafka_partition DESC,
            kafka_offset DESC,
            ingestion_timestamp DESC
    )=1
)

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
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