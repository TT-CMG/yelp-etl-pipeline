CREATE TABLE IF NOT EXISTS dw.dim_time (
    time_key INT PRIMARY KEY,
    full_date TIMESTAMP NOT NULL,
    year INT,
    quarter INT,
    month INT,
    day INT,
    day_of_week INT,
    week_of_year INT,
    hour INT,
    month_name VARCHAR(20),
    day_name VARCHAR(20),
    is_weekend BOOLEAN
);
