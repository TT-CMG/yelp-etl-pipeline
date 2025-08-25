CREATE TABLE IF NOT EXISTS dw.fact_tip (
    tip_key SERIAL PRIMARY KEY,
    user_key INT REFERENCES dw.dim_user(user_key),
    business_key INT REFERENCES dw.dim_business(business_key),
    time_key INT REFERENCES dw.dim_time(time_key),
    text_length INT,
    likes INT
);
