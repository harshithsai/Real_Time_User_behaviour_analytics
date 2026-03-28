-- models/marts/agg_hourly_kpis.sql
-- Hourly aggregated KPIs — this is what the Streamlit dashboard queries
-- Clustered by hour_timestamp for fast time-range scans

WITH hourly_clicks AS (
    SELECT
        DATE_TRUNC('hour', clicked_at)  AS hour_timestamp,
        country,
        device,
        product_category,
        COUNT(*)                         AS total_clicks,
        COUNT(DISTINCT user_id)          AS unique_users,
        COUNT(DISTINCT session_id)       AS unique_sessions,
        SUM(CASE WHEN is_mobile THEN 1 ELSE 0 END) AS mobile_clicks,
        AVG(price_seen_usd)              AS avg_price_seen_usd
    FROM {{ ref('stg_clicks') }}
    GROUP BY 1, 2, 3, 4
),

hourly_revenue AS (
    SELECT
        DATE_TRUNC('hour', purchased_at) AS hour_timestamp,
        country,
        product_category,
        COUNT(DISTINCT order_id)          AS total_orders,
        SUM(total_amount_usd)             AS total_revenue_usd,
        AVG(total_amount_usd)             AS avg_order_value_usd,
        COUNT(DISTINCT user_id)           AS purchasing_users,
        SUM(quantity)                     AS total_items_sold
    FROM {{ ref('stg_purchases') }}
    GROUP BY 1, 2, 3
),

hourly_sessions AS (
    SELECT
        DATE_TRUNC('hour', session_start) AS hour_timestamp,
        country,
        device,
        COUNT(*)                           AS total_sessions,
        SUM(CASE WHEN is_bounce THEN 1 ELSE 0 END) AS bounced_sessions,
        AVG(duration_seconds)              AS avg_session_duration_seconds,
        SUM(CASE WHEN is_new_user THEN 1 ELSE 0 END) AS new_user_sessions
    FROM {{ ref('stg_sessions') }}
    GROUP BY 1, 2, 3
),

combined AS (
    SELECT
        c.hour_timestamp,
        c.country,
        c.device,
        c.product_category,

        -- Click metrics
        c.total_clicks,
        c.unique_users,
        c.unique_sessions,
        c.mobile_clicks,
        c.avg_price_seen_usd,
        ROUND(c.mobile_clicks * 100.0 / NULLIF(c.total_clicks, 0), 2) AS mobile_click_pct,

        -- Revenue metrics
        COALESCE(r.total_orders, 0)         AS total_orders,
        COALESCE(r.total_revenue_usd, 0)    AS total_revenue_usd,
        COALESCE(r.avg_order_value_usd, 0)  AS avg_order_value_usd,
        COALESCE(r.purchasing_users, 0)     AS purchasing_users,
        COALESCE(r.total_items_sold, 0)     AS total_items_sold,

        -- Session metrics
        COALESCE(s.total_sessions, 0)       AS total_sessions,
        COALESCE(s.bounced_sessions, 0)     AS bounced_sessions,
        COALESCE(s.avg_session_duration_seconds, 0) AS avg_session_duration_seconds,
        COALESCE(s.new_user_sessions, 0)    AS new_user_sessions,

        -- Derived KPIs
        ROUND(
            COALESCE(r.total_orders, 0) * 100.0 / NULLIF(c.unique_sessions, 0),
            2
        )                                   AS conversion_rate_pct,

        ROUND(
            COALESCE(r.total_revenue_usd, 0) / NULLIF(c.total_clicks, 0),
            4
        )                                   AS revenue_per_click_usd,

        ROUND(
            COALESCE(s.bounced_sessions, 0) * 100.0 / NULLIF(COALESCE(s.total_sessions, 0), 0),
            2
        )                                   AS bounce_rate_pct,

        ROUND(
            COALESCE(s.new_user_sessions, 0) * 100.0 / NULLIF(COALESCE(s.total_sessions, 0), 0),
            2
        )                                   AS new_user_rate_pct,

        CURRENT_TIMESTAMP()                 AS dbt_updated_at

    FROM hourly_clicks c
    LEFT JOIN hourly_revenue r
        ON  c.hour_timestamp    = r.hour_timestamp
        AND c.country           = r.country
        AND c.product_category  = r.product_category
    LEFT JOIN hourly_sessions s
        ON  c.hour_timestamp    = s.hour_timestamp
        AND c.country           = s.country
        AND c.device            = s.device
)

SELECT * FROM combined
ORDER BY hour_timestamp DESC