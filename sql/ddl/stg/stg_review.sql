CREATE TABLE IF NOT EXISTS stg.stg_review (
    business_id TEXT,
    cool INT,
    review_date TIMESTAMP,
    funny INT,
    review_id TEXT,
    stars NUMERIC(2,1),
    text TEXT,
    useful INT,
    user_id TEXT
);
