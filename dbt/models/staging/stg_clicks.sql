-- models/staging/stg_clicks.sql
-- Cleans and types raw click events from RAW_DB
-- Materialized as a VIEW — no storage cost, always reflects latest raw data

WITH source AS (
    SELECT * FROM {{ source('raw_events', 'RAW_CLICKS') }}
),

renamed AS (
    SELECT
        EVENT_ID          AS click_id,
        EVENT_TYPE        AS event_type,
        USER_ID           AS user_id,
        SESSION_ID        AS session_id,
        PRODUCT_ID        AS product_id,
        UPPER(TRIM(PRODUCT_NAME))     AS product_name,
        UPPER(TRIM(PRODUCT_CATEGORY)) AS product_category,
        LOWER(TRIM(PAGE))             AS page,
        LOWER(TRIM(DEVICE))           AS device,
        UPPER(TRIM(COUNTRY))          AS country,
        LOWER(TRIM(REFERRER))         AS referrer,
        ROUND(PRICE_SEEN, 2)          AS price_seen_usd,

        EVENT_TIMESTAMP::TIMESTAMP_TZ AS clicked_at,
        DATE(EVENT_TIMESTAMP)         AS click_date,
        HOUR(EVENT_TIMESTAMP)         AS click_hour,

        -- Derived fields
        CASE
            WHEN LOWER(TRIM(DEVICE)) = 'mobile' THEN TRUE
            ELSE FALSE
        END AS is_mobile,

        CASE
            WHEN LOWER(TRIM(PAGE)) IN ('cart', 'checkout') THEN TRUE
            ELSE FALSE
        END AS is_high_intent_page,

        -- Data quality flag — used to filter bad records before marts
        CASE
            WHEN EVENT_ID IS NULL       THEN 'missing_event_id'
            WHEN USER_ID IS NULL        THEN 'missing_user_id'
            WHEN SESSION_ID IS NULL     THEN 'missing_session_id'
            WHEN EVENT_TIMESTAMP IS NULL THEN 'missing_timestamp'
            WHEN PRICE_SEEN < 0         THEN 'negative_price'
            ELSE 'valid'
        END AS data_quality_flag

    FROM source
    WHERE EVENT_TIMESTAMP >= DATEADD(day, -90, CURRENT_TIMESTAMP())  -- 90-day lookback
)

SELECT * FROM renamed
WHERE data_quality_flag = 'valid'  -- Only pass clean records downstream