-- models/marts/fct_sessions.sql
-- Session-level fact table: one row per user session
-- Joins clicks and purchases to get full session picture

WITH sessions AS (
    SELECT * FROM {{ ref('stg_sessions') }}
),

session_clicks AS (
    SELECT
        session_id,
        COUNT(*)                                AS total_clicks,
        COUNT(DISTINCT product_id)              AS unique_products_viewed,
        COUNT(DISTINCT page)                    AS unique_pages_visited,
        MAX(price_seen_usd)                     AS max_price_seen,
        SUM(CASE WHEN page = 'cart' THEN 1 ELSE 0 END) AS cart_views,
        SUM(CASE WHEN page = 'checkout' THEN 1 ELSE 0 END) AS checkout_views
    FROM {{ ref('stg_clicks') }}
    GROUP BY session_id
),

session_purchases AS (
    SELECT
        session_id,
        COUNT(*)                                AS total_orders,
        SUM(total_amount_usd)                   AS total_revenue,
        SUM(quantity)                           AS total_items_purchased,
        TRUE                                    AS did_purchase
    FROM {{ ref('stg_purchases') }}
    GROUP BY session_id
),

final AS (
    SELECT
        s.session_id,
        s.user_id,
        s.session_start                         AS session_started_at,
        s.duration_seconds,
        ROUND(s.duration_seconds / 60.0, 2)     AS duration_minutes,
        s.is_new_user,
        s.device,
        s.country,
        s.session_date,

        -- Click behavior
        COALESCE(sc.total_clicks, 0)            AS total_clicks,
        COALESCE(sc.unique_products_viewed, 0)  AS unique_products_viewed,
        COALESCE(sc.unique_pages_visited, s.pages_visited) AS pages_visited,
        COALESCE(sc.cart_views, 0)              AS cart_views,
        COALESCE(sc.checkout_views, 0)          AS checkout_views,
        COALESCE(sc.max_price_seen, 0)          AS max_price_seen_usd,

        -- Purchase outcome
        COALESCE(sp.did_purchase, FALSE)        AS did_purchase,
        COALESCE(sp.total_orders, 0)            AS total_orders,
        COALESCE(sp.total_revenue, 0)           AS session_revenue_usd,
        COALESCE(sp.total_items_purchased, 0)   AS items_purchased,

        -- Engagement score (custom metric)
        ROUND(
            (COALESCE(sc.total_clicks, 0) * 1.0) +
            (COALESCE(sc.cart_views, 0) * 3.0) +
            (COALESCE(sp.did_purchase, FALSE)::INT * 10.0),
        2)                                      AS engagement_score,

        CURRENT_TIMESTAMP()                     AS dbt_updated_at

    FROM sessions s
    LEFT JOIN session_clicks sc ON s.session_id = sc.session_id
    LEFT JOIN session_purchases sp ON s.session_id = sp.session_id
)

SELECT * FROM final
