-- tests/unique_session_id.sql
-- Custom test: fails if any session_id appears more than once in fct_sessions

SELECT
    session_id,
    COUNT(*) AS row_count
FROM {{ ref('fct_sessions') }}
GROUP BY session_id
HAVING COUNT(*) > 1