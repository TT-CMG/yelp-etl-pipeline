CREATE TABLE IF NOT EXISTS dw.dim_business (
    business_key SERIAL PRIMARY KEY,
    business_id VARCHAR(50) UNIQUE NOT NULL,
    name TEXT,
    stars NUMERIC,
    review_count INT,
    is_open BOOLEAN,
    location_key INT REFERENCES dw.dim_location(location_key)
);


CREATE TABLE IF NOT EXISTS dw.dim_category ( 
    category_key SERIAL PRIMARY KEY, 
    category_name TEXT UNIQUE 
); 

CREATE TABLE IF NOT EXISTS dw.bridge_business_category ( 
    business_key INT REFERENCES dw.dim_business(business_key), 
    category_key INT REFERENCES dw.dim_category(category_key), 
    PRIMARY KEY (business_key, category_key) 

);