SELECT
    year,
    vehicle_type,
    COUNT(*) AS occurrences

FROM {{ ref('mart_vehicle_trends') }}

GROUP BY
    year,
    vehicle_type

HAVING COUNT(*) > 1