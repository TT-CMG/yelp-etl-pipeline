-- -- Chèn các row mới
-- INSERT INTO dw.dim_user (user_id, name, yelping_since, review_count,
--     useful, funny, cool, fans, average_stars, total_compliments)
-- SELECT user_id, name, yelping_since, review_count,
--        useful, funny, cool, fans, average_stars, total_compliments
-- FROM stg.stg_user_temp
-- WHERE NOT EXISTS (
--     SELECT 1 FROM dw.dim_user d WHERE d.user_id = stg_user_temp.user_id
-- );

-- -- Cập nhật row đã tồn tại
-- UPDATE dw.dim_user d
-- SET name = s.name,
--     yelping_since = s.yelping_since,
--     review_count = s.review_count,
--     useful = s.useful,
--     funny = s.funny,
--     cool = s.cool,
--     fans = s.fans,
--     average_stars = s.average_stars,
--     total_compliments = s.total_compliments
-- FROM stg.stg_user_temp s
-- WHERE d.user_id = s.user_id;

INSERT INTO dw.dim_user (
    user_id, name, yelping_since, review_count,
    useful, funny, cool, fans, average_stars, total_compliments
)
SELECT user_id, name, yelping_since, review_count,
       useful, funny, cool, fans, average_stars, total_compliments
FROM stg.stg_user_temp;
