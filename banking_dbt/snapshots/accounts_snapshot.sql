
{% snapshot accounts_snapshot %}

{{
    config(
        target_database='BANKING_DWH',
        target_schema='SNAPSHOTS',
        unique_key='account_id',
        strategy='timestamp',

        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

SELECT * FROM {{ ref('stg_accounts') }}

--
{% endsnapshot %}