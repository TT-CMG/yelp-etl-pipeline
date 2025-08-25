CREATE TABLE IF NOT EXISTS dw.dim_location (
    location_key SERIAL PRIMARY KEY,
    address TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    latitude NUMERIC,
    longitude NUMERIC
);
