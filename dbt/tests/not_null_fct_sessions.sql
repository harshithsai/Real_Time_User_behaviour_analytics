-- tests/not_null_critical_fields.sql
-- Custom test: fails (returns rows) if any critical field is null
-- dbt treats any rows returned = test failure

SELECT session_id, user_id, 'null_session_id' AS test_name
FROM {{ ref('fct_sessions') }}
WHERE session_id IS NULL

UNION ALL

SELECT session_id, user_id, 'null_user_id' AS test_name
FROM {{ ref('fct_sessions') }}
WHERE user_id IS NULL

UNION ALL

SELECT session_id, user_id, 'negative_revenue' AS test_name
FROM {{ ref('fct_sessions') }}
WHERE session_revenue_usd < 0

UNION ALL

SELECT session_id, user_id, 'negative_engagement_score' AS test_name
FROM {{ ref('fct_sessions') }}
WHERE engagement_score < 0