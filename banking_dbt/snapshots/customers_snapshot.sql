
{% snapshot customers_snapshot %}

{{
    config(
        target_database='BANKING_DWH',
        target_schema='SNAPSHOTS',
        unique_key='customer_id',
        strategy='timestamp',

        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

SELECT * FROM {{ ref('stg_customers') }} WHERE cdc_operation <> 'd' OR cdc_operation IS NULL

--
{% endsnapshot %}