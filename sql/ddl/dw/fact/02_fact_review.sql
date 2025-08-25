CREATE TABLE IF NOT EXISTS dw.fact_review (
    review_key SERIAL PRIMARY KEY,
    review_id VARCHAR(50) UNIQUE NOT NULL,
    user_key INT REFERENCES dw.dim_user(user_key),
    business_key INT REFERENCES dw.dim_business(business_key),
    time_key INT REFERENCES dw.dim_time(time_key),
    stars INT,
    useful INT,
    funny INT,
    cool INT,
    text_length INT
);
