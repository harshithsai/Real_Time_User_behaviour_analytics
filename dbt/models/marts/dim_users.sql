-- models/marts/dim_users.sql
-- User dimension table: one row per user with lifetime metrics
-- Aggregates all sessions and purchases to build a user profile
-- NOTE: This model was missing from the original guide

WITH user_sessions AS (
    SELECT
        user_id,
        COUNT(*)                                        AS total_sessions,
        SUM(total_clicks)                               AS lifetime_clicks,
        SUM(CASE WHEN is_new_user THEN 1 ELSE 0 END)   AS new_user_sessions,
        SUM(CASE WHEN is_bounce THEN 1 ELSE 0 END)      AS bounce_sessions,
        AVG(duration_seconds)                           AS avg_session_duration_seconds,
        MAX(duration_seconds)                           AS max_session_duration_seconds,
        MIN(session_date)                               AS first_seen_date,
        MAX(session_date)                               AS last_seen_date,
        -- Most common device and country
        MODE(device)                                    AS primary_device,
        MODE(country)                                   AS primary_country,
        -- Engagement
        AVG(engagement_score)                           AS avg_engagement_score,
        SUM(CASE WHEN did_purchase THEN 1 ELSE 0 END)  AS sessions_with_purchase,
        SUM(session_revenue_usd)                        AS lifetime_revenue_usd,
        COUNT(DISTINCT country)                         AS countries_visited_from
    FROM {{ ref('fct_sessions') }}
    GROUP BY user_id
),

user_purchases AS (
    SELECT
        user_id,
        COUNT(DISTINCT order_id)                        AS total_orders,
        SUM(total_amount_usd)                           AS total_spend_usd,
        AVG(total_amount_usd)                           AS avg_order_value_usd,
        MAX(total_amount_usd)                           AS max_order_value_usd,
        MIN(purchase_date)                              AS first_purchase_date,
        MAX(purchase_date)                              AS last_purchase_date,
        MODE(product_category)                          AS favorite_category,
        MODE(payment_group)                             AS preferred_payment_group,
        COUNT(DISTINCT product_category)                AS unique_categories_purchased
    FROM {{ ref('fct_purchases') }}
    GROUP BY user_id
),

final AS (
    SELECT
        -- Identifier
        s.user_id,

        -- Activity window
        s.first_seen_date,
        s.last_seen_date,
        DATEDIFF(day, s.first_seen_date, s.last_seen_date) AS days_active,

        -- Session behaviour
        s.total_sessions,
        s.lifetime_clicks,
        s.bounce_sessions,
        ROUND(s.bounce_sessions * 100.0 / NULLIF(s.total_sessions, 0), 2) AS bounce_rate_pct,
        ROUND(s.avg_session_duration_seconds / 60.0, 2)  AS avg_session_duration_minutes,
        s.primary_device,
        s.primary_country,
        s.avg_engagement_score,
        s.countries_visited_from,

        -- Purchase behaviour
        COALESCE(p.total_orders, 0)                     AS total_orders,
        COALESCE(p.total_spend_usd, 0)                  AS lifetime_spend_usd,
        COALESCE(p.avg_order_value_usd, 0)              AS avg_order_value_usd,
        COALESCE(p.max_order_value_usd, 0)              AS max_order_value_usd,
        p.first_purchase_date,
        p.last_purchase_date,
        p.favorite_category,
        p.preferred_payment_group,
        COALESCE(p.unique_categories_purchased, 0)      AS unique_categories_purchased,

        -- Conversion metrics
        COALESCE(s.sessions_with_purchase, 0)           AS sessions_with_purchase,
        ROUND(
            COALESCE(s.sessions_with_purchase, 0) * 100.0 / NULLIF(s.total_sessions, 0),
            2
        )                                               AS conversion_rate_pct,

        -- User segment based on lifetime value
        CASE
            WHEN COALESCE(p.total_spend_usd, 0) >= 1000 THEN 'vip'
            WHEN COALESCE(p.total_spend_usd, 0) >= 200  THEN 'regular'
            WHEN COALESCE(p.total_orders, 0) >= 1        THEN 'one_time'
            ELSE 'browser'
        END                                             AS user_segment,

        -- Is the user a buyer at all?
        CASE WHEN COALESCE(p.total_orders, 0) > 0 THEN TRUE ELSE FALSE END AS is_buyer,

        CURRENT_TIMESTAMP()                             AS dbt_updated_at

    FROM user_sessions s
    LEFT JOIN user_purchases p ON s.user_id = p.user_id
)

SELECT * FROM final