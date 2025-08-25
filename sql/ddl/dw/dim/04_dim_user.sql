CREATE TABLE IF NOT EXISTS dw.dim_user (
    user_key SERIAL PRIMARY KEY,
    user_id VARCHAR(50) UNIQUE NOT NULL,
    name TEXT,

    yelping_since DATE,
    review_count INT,

    useful INT,
    funny INT,
    cool INT,

    fans INT,

    total_compliments INT DEFAULT 0,
    average_stars NUMERIC(3,2)
);
