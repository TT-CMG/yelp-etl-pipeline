CREATE TABLE IF NOT EXISTS dw.fact_checkin (
    checkin_key SERIAL PRIMARY KEY,
    business_key INT REFERENCES dw.dim_business(business_key),
    time_key INT REFERENCES dw.dim_time(time_key),
    checkin_count INT
);
