SELECT
customer_id,
COUNT(*)
FROM {{ ref('dim_customers') }}
WHERE is_current = TRUE
GROUP BY customer_id
HAVING COUNT(*) > 1