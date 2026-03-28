-- models/staging/stg_purchases.sql
-- Cleans and types raw purchase events from RAW_DB
-- Materialized as a VIEW — no storage cost, always reflects latest raw data

WITH source AS (
    SELECT * FROM {{ source('raw_events', 'RAW_PURCHASES') }}
),

cleaned AS (
    SELECT
        EVENT_ID                            AS purchase_id,
        ORDER_ID                            AS order_id,
        USER_ID                             AS user_id,
        SESSION_ID                          AS session_id,
        PRODUCT_ID                          AS product_id,
        UPPER(TRIM(PRODUCT_NAME))           AS product_name,
        UPPER(TRIM(PRODUCT_CATEGORY))       AS product_category,

        QUANTITY                            AS quantity,
        ROUND(UNIT_PRICE, 2)               AS unit_price_usd,
        ROUND(TOTAL_AMOUNT, 2)             AS total_amount_usd,

        LOWER(TRIM(PAYMENT_METHOD))         AS payment_method,
        UPPER(TRIM(COUNTRY))                AS country,
        IS_FRAUDULENT                       AS is_fraudulent,

        EVENT_TIMESTAMP::TIMESTAMP_TZ       AS purchased_at,
        DATE(EVENT_TIMESTAMP)               AS purchase_date,
        HOUR(EVENT_TIMESTAMP)               AS purchase_hour,

        -- Revenue tier segmentation
        CASE
            WHEN TOTAL_AMOUNT >= 200 THEN 'high_value'
            WHEN TOTAL_AMOUNT >= 50  THEN 'mid_value'
            ELSE 'low_value'
        END AS order_value_tier,

        -- Payment method grouping
        CASE
            WHEN LOWER(TRIM(PAYMENT_METHOD)) IN ('credit_card', 'debit_card') THEN 'card'
            WHEN LOWER(TRIM(PAYMENT_METHOD)) IN ('apple_pay', 'google_pay')   THEN 'digital_wallet'
            WHEN LOWER(TRIM(PAYMENT_METHOD)) = 'paypal'                        THEN 'paypal'
            ELSE 'other'
        END AS payment_group,

        -- Data quality flag
        CASE
            WHEN EVENT_ID IS NULL        THEN 'missing_event_id'
            WHEN ORDER_ID IS NULL        THEN 'missing_order_id'
            WHEN USER_ID IS NULL         THEN 'missing_user_id'
            WHEN EVENT_TIMESTAMP IS NULL THEN 'missing_timestamp'
            WHEN TOTAL_AMOUNT <= 0       THEN 'zero_or_negative_amount'
            WHEN QUANTITY <= 0           THEN 'zero_or_negative_quantity'
            ELSE 'valid'
        END AS data_quality_flag

    FROM source
    WHERE
        EVENT_TIMESTAMP >= DATEADD(day, -90, CURRENT_TIMESTAMP())
        AND IS_FRAUDULENT = FALSE   -- Exclude fraud from analytics layer
)

SELECT * FROM cleaned
WHERE data_quality_flag = 'valid'