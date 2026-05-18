-- Keep device-library voltage ratings consistent with rdson_mohm/current_rating_a:
-- store only numeric text, with volts carried by the column label.

WITH normalized AS (
    SELECT
        id,
        regexp_replace(
            regexp_replace(
                (
                    regexp_replace(
                        voltage_rating,
                        '^[[:space:]]*([+-]?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?)[[:space:]]*(k[[:space:]]*v|kv|v|volts?)[[:space:]]*$',
                        '\1',
                        'i'
                    )::numeric
                    * CASE
                        WHEN voltage_rating ~* '^[[:space:]]*[+-]?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?[[:space:]]*k[[:space:]]*v[[:space:]]*$'
                        THEN 1000
                        ELSE 1
                      END
                )::text,
                '(\.[0-9]*[1-9])0+$',
                '\1'
            ),
            '\.0+$',
            ''
        ) AS voltage_rating
    FROM device_library
    WHERE voltage_rating ~* '^[[:space:]]*[+-]?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?[[:space:]]*(k[[:space:]]*v|kv|v|volts?)[[:space:]]*$'
)
UPDATE device_library dl
SET voltage_rating = normalized.voltage_rating
FROM normalized
WHERE dl.id = normalized.id
  AND dl.voltage_rating IS DISTINCT FROM normalized.voltage_rating;
