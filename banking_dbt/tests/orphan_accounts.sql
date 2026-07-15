-- tests/orphan_accounts.sql


SELECT a.*
FROM {{ ref('dim_accounts') }} a
LEFT JOIN {{ ref('dim_customers') }} c
ON a.customer_id = c.customer_id

WHERE c.customer_id IS NULL