-- -- load_dim_time.sql
-- INSERT INTO dw.dim_time (date, year, quarter, month, day, weekday)
-- SELECT DISTINCT
--     CAST(s.date AS DATE) AS date,
--     EXTRACT(YEAR FROM s.date)    AS year,
--     EXTRACT(QUARTER FROM s.date) AS quarter,
--     EXTRACT(MONTH FROM s.date)   AS month,
--     EXTRACT(DAY FROM s.date)     AS day,
--     EXTRACT(DOW FROM s.date)     AS weekday
-- FROM (
--     SELECT review_date AS date FROM stg.stg_review
--     UNION
--     SELECT tip_date    AS date FROM stg.stg_tip
--     UNION
--     SELECT checkin_date AS date FROM stg.stg_checkin
-- ) s
-- LEFT JOIN dw.dim_time t
--        ON t.date = CAST(s.date AS DATE)
-- WHERE t.time_key IS NULL;

INSERT INTO dw.dim_time(full_date, year, quarter, month, day, day_of_week)

SELECT DISTINCT
    CAST(s.full_date as date),
    EXTRACT(YEAR from s.full_date) as year,
    EXTRACT(QUARTER FROM s.full_date) AS quarter,
    EXTRACT(MONTH FROM s.full_date)   AS month,
    EXTRACT(DAY FROM s.full_date)     AS day,
    EXTRACT(DOW FROM s.full_date)     AS day_of_week
FROM (
    SELECT review_date AS full_date FROM stg.stg_review
    UNION
    SELECT tip_date AS full_date FROM stg.stg_tip
    UNION
    SELECT unnest(string_to_array(checkin_date, ', '))::timestamp::date AS full_date
    FROM stg.stg_checkin
) as s
LEFT JOIN dw.dim_time as t on t.full_date = CAST(s.full_date as DATE)
WHERE t.time_key IS NULL;
