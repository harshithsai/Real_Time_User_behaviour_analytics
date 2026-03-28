-- models/staging/stg_sessions.sql
-- Cleans and types raw session events from RAW_DB
-- Materialized as a VIEW — no storage cost, always reflects latest raw data
-- NOTE: This model was missing from the original guide — written to match
--       RAW_SESSIONS schema exactly (10 columns, no INGESTED_AT)

WITH source AS (
    SELECT * FROM {{ source('raw_events', 'RAW_SESSIONS') }}
),

cleaned AS (
    SELECT
        EVENT_ID                        AS session_event_id,
        SESSION_ID                      AS session_id,
        USER_ID                         AS user_id,

        SESSION_START::TIMESTAMP_TZ     AS session_start,
        EVENT_TIMESTAMP::TIMESTAMP_TZ   AS event_timestamp,
        DATE(EVENT_TIMESTAMP)           AS session_date,
        HOUR(EVENT_TIMESTAMP)           AS session_hour,

        DURATION_SECONDS                AS duration_seconds,
        ROUND(DURATION_SECONDS / 60.0, 2) AS duration_minutes,
        PAGES_VISITED                   AS pages_visited,

        LOWER(TRIM(DEVICE))             AS device,
        UPPER(TRIM(COUNTRY))            AS country,
        IS_NEW_USER                     AS is_new_user,

        -- Session engagement tier based on duration
        CASE
            WHEN DURATION_SECONDS >= 600 THEN 'highly_engaged'   -- 10+ minutes
            WHEN DURATION_SECONDS >= 120 THEN 'engaged'           -- 2-10 minutes
            WHEN DURATION_SECONDS >= 30  THEN 'brief'             -- 30s-2 minutes
            ELSE 'bounce'                                          -- under 30 seconds
        END AS engagement_tier,

        -- Bounce flag: single page, short duration
        CASE
            WHEN PAGES_VISITED <= 1 AND DURATION_SECONDS < 30 THEN TRUE
            ELSE FALSE
        END AS is_bounce,

        -- Data quality flag
        CASE
            WHEN EVENT_ID IS NULL       THEN 'missing_event_id'
            WHEN SESSION_ID IS NULL     THEN 'missing_session_id'
            WHEN USER_ID IS NULL        THEN 'missing_user_id'
            WHEN EVENT_TIMESTAMP IS NULL THEN 'missing_timestamp'
            WHEN DURATION_SECONDS < 0   THEN 'negative_duration'
            WHEN PAGES_VISITED < 0      THEN 'negative_pages'
            ELSE 'valid'
        END AS data_quality_flag

    FROM source
    WHERE EVENT_TIMESTAMP >= DATEADD(day, -90, CURRENT_TIMESTAMP())
)

SELECT * FROM cleaned
WHERE data_quality_flag = 'valid'