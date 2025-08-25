CREATE TABLE IF NOT EXISTS stg.stg_business (
    business_id TEXT,
    name TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    stars NUMERIC(2,1),
    review_count INT,
    is_open INT,
    attributes JSONB,
    categories TEXT,
    hours JSONB
);
