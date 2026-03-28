-- models/marts/fct_purchases.sql
-- Purchase-level fact table: one row per order
-- Enriched with session context for full purchase picture
-- Clustered by purchase_date + product_category for fast revenue queries
-- NOTE: This model was missing from the original guide

WITH purchases AS (
    SELECT * FROM {{ ref('stg_purchases') }}
),

session_context AS (
    -- Pull session attributes to enrich each purchase with context
    SELECT
        session_id,
        is_new_user,
        engagement_tier,
        total_clicks,
        cart_views,
        checkout_views,
        duration_seconds,
        reached_cart,
        reached_checkout
    FROM {{ ref('int_sessionized_events') }}
),

final AS (
    SELECT
        -- Identifiers
        p.purchase_id,
        p.order_id,
        p.user_id,
        p.session_id,
        p.product_id,

        -- Product info
        p.product_name,
        p.product_category,

        -- Purchase details
        p.quantity,
        p.unit_price_usd,
        p.total_amount_usd,
        p.payment_method,
        p.payment_group,
        p.country,
        p.order_value_tier,

        -- Timestamps
        p.purchased_at,
        p.purchase_date,
        p.purchase_hour,

        -- Session context (how did the user behave before buying?)
        COALESCE(s.is_new_user, FALSE)          AS is_new_user,
        COALESCE(s.engagement_tier, 'unknown')  AS session_engagement_tier,
        COALESCE(s.total_clicks, 0)             AS session_total_clicks,
        COALESCE(s.cart_views, 0)               AS session_cart_views,
        COALESCE(s.checkout_views, 0)           AS session_checkout_views,
        COALESCE(s.duration_seconds, 0)         AS session_duration_seconds,
        COALESCE(s.reached_cart, FALSE)         AS session_reached_cart,
        COALESCE(s.reached_checkout, FALSE)     AS session_reached_checkout,

        -- Revenue breakdown
        ROUND(p.unit_price_usd * p.quantity, 2) AS gross_revenue_usd,
        p.total_amount_usd                       AS net_revenue_usd,

        -- Day of week for seasonality analysis
        DAYNAME(p.purchased_at)                  AS day_of_week,
        CASE
            WHEN DAYOFWEEK(p.purchased_at) IN (1, 7) THEN TRUE
            ELSE FALSE
        END                                      AS is_weekend,

        CURRENT_TIMESTAMP()                      AS dbt_updated_at

    FROM purchases p
    LEFT JOIN session_context s ON p.session_id = s.session_id
)

SELECT * FROM final