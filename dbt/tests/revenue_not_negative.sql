-- tests/revenue_not_negative.sql
-- Custom test: fails if any purchase has negative or zero revenue

SELECT
    purchase_id,
    order_id,
    total_amount_usd,
    'negative_or_zero_revenue' AS test_name
FROM {{ ref('fct_purchases') }}
WHERE total_amount_usd <= 0

UNION ALL

SELECT
    purchase_id,
    order_id,
    net_revenue_usd,
    'negative_net_revenue' AS test_name
FROM {{ ref('fct_purchases') }}
WHERE net_revenue_usd <= 0