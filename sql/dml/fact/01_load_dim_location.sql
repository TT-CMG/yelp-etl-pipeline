INSERT INTO dw.dim_location(address, city, state, postal_code, latitude, longitude)

SELECT DISTINCT
    s.address,
    s.city,
    s.state,
    s.postal_code,
    s.latitude,
    s.longitude
FROM stg.stg_business as s LEFT JOIN dw.dim_location as d
ON      s.address = d.address
    AND s.city = d.city
    AND s.state = d.state
    AND s.postal_code = d.postal_code
    AND s.latitude = d.latitude
    AND s.longitude = d.longitude
WHERE d.location_key IS NULL;
