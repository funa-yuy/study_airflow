-- マートテーブル作成用 SQL
CREATE TABLE mart_schema.user_summary AS
SELECT
    u.user_id,
    u.user_name,
    u.registered_at,
    COUNT(e.event_id) AS total_events,
    MAX(e.event_time) AS last_event_time
FROM
    raw_schema.raw_users u
LEFT JOIN
    raw_schema.raw_events e ON u.user_id = e.user_id
GROUP BY
    u.user_id,
    u.user_name,
    u.registered_at;
