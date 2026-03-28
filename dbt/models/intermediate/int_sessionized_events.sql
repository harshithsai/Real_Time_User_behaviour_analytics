-- models/intermediate/int_sessionized_events.sql
-- Intermediate model: joins clicks + sessions into one sessionized view
-- Materialized as EPHEMERAL — compiled as a CTE, no table created in Snowflake
-- NOTE: This model was missing from the original guide

WITH sessions AS (
    SELECT * FROM {{ ref('stg_sessions') }}
),

clicks AS (
    SELECT * FROM {{ ref('stg_clicks') }}
),

-- Aggregate click metrics per session
click_metrics AS (
    SELECT
        session_id,
        COUNT(*)                                                AS total_clicks,
        COUNT(DISTINCT user_id)                                 AS unique_users_in_session,
        COUNT(DISTINCT product_id)                              AS unique_products_viewed,
        COUNT(DISTINCT page)                                    AS unique_pages_visited,
        COUNT(DISTINCT product_category)                        AS unique_categories_viewed,
        MAX(price_seen_usd)                                     AS max_price_seen_usd,
        AVG(price_seen_usd)                                     AS avg_price_seen_usd,
        SUM(CASE WHEN page = 'cart'     THEN 1 ELSE 0 END)     AS cart_views,
        SUM(CASE WHEN page = 'checkout' THEN 1 ELSE 0 END)     AS checkout_views,
        SUM(CASE WHEN page = 'product'  THEN 1 ELSE 0 END)     AS product_page_views,
        SUM(CASE WHEN is_mobile = TRUE  THEN 1 ELSE 0 END)     AS mobile_clicks,
        MIN(clicked_at)                                         AS first_click_at,
        MAX(clicked_at)                                         AS last_click_at,
        -- Funnel progression flags
        MAX(CASE WHEN page = 'cart'     THEN 1 ELSE 0 END)     AS reached_cart,
        MAX(CASE WHEN page = 'checkout' THEN 1 ELSE 0 END)     AS reached_checkout
    FROM clicks
    GROUP BY session_id
),

-- Join session data with click aggregates
sessionized AS (
    SELECT
        -- Session identifiers
        s.session_id,
        s.user_id,
        s.session_date,
        s.session_hour,
        s.session_start,
        s.event_timestamp,

        -- Session attributes
        s.device,
        s.country,
        s.is_new_user,
        s.is_bounce,
        s.engagement_tier,
        s.duration_seconds,
        s.duration_minutes,
        s.pages_visited                                         AS reported_pages_visited,

        -- Click metrics (from actual click events)
        COALESCE(c.total_clicks, 0)                            AS total_clicks,
        COALESCE(c.unique_products_viewed, 0)                  AS unique_products_viewed,
        COALESCE(c.unique_categories_viewed, 0)                AS unique_categories_viewed,
        COALESCE(c.unique_pages_visited, s.pages_visited)      AS actual_pages_visited,
        COALESCE(c.cart_views, 0)                              AS cart_views,
        COALESCE(c.checkout_views, 0)                          AS checkout_views,
        COALESCE(c.product_page_views, 0)                      AS product_page_views,
        COALESCE(c.max_price_seen_usd, 0)                      AS max_price_seen_usd,
        COALESCE(c.avg_price_seen_usd, 0)                      AS avg_price_seen_usd,
        COALESCE(c.mobile_clicks, 0)                           AS mobile_clicks,

        -- Funnel flags
        COALESCE(c.reached_cart, 0) = 1                        AS reached_cart,
        COALESCE(c.reached_checkout, 0) = 1                    AS reached_checkout,

        -- Engagement score (used in fct_sessions)
        ROUND(
            (COALESCE(c.total_clicks, 0) * 1.0) +
            (COALESCE(c.cart_views, 0) * 3.0) +
            (COALESCE(c.checkout_views, 0) * 5.0),
            2
        )                                                       AS pre_purchase_engagement_score

    FROM sessions s
    LEFT JOIN click_metrics c ON s.session_id = c.session_id
)

SELECT * FROM sessionized