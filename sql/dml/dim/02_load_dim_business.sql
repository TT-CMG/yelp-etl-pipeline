WITH ranked AS (
    SELECT
        s.*,
        l.location_key,
        ROW_NUMBER() OVER (PARTITION BY s.business_id ORDER BY s.review_count DESC) AS rn
    FROM stg.stg_business s
    LEFT JOIN dw.dim_location l 
        ON s.address = l.address
       AND s.city = l.city
       AND s.state = l.state
       AND s.postal_code = l.postal_code
)
INSERT INTO dw.dim_business (
    business_id, name, category, stars, review_count, is_open, location_key
)
SELECT
    business_id,
    name,
    categories,
    stars::NUMERIC,
    review_count::INT,
    is_open::BOOLEAN,
    location_key
FROM ranked
WHERE rn = 1
ON CONFLICT (business_id) DO UPDATE
SET 
    name         = COALESCE(EXCLUDED.name, dw.dim_business.name),
    category     = COALESCE(EXCLUDED.category, dw.dim_business.category),
    stars        = COALESCE(EXCLUDED.stars, dw.dim_business.stars),
    review_count = COALESCE(EXCLUDED.review_count, dw.dim_business.review_count),
    is_open      = COALESCE(EXCLUDED.is_open, dw.dim_business.is_open),
    location_key = COALESCE(EXCLUDED.location_key, dw.dim_business.location_key);
