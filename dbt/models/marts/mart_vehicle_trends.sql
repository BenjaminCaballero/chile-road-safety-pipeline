WITH yearly_data AS (

    SELECT
        year,
        vehicle_type,
        crashes,
        fatalities,

        LAG(year) OVER (
            PARTITION BY vehicle_type
            ORDER BY year
        ) AS previous_year,

        LAG(crashes) OVER (
            PARTITION BY vehicle_type
            ORDER BY year
        ) AS previous_crashes

    FROM {{ ref('stg_vehicle_safety') }}

)

SELECT
    year,
    vehicle_type,
    crashes,
    fatalities,
    previous_year,
    previous_crashes,

    CASE
        WHEN previous_year = year - 1
             AND previous_crashes > 0

        THEN ROUND(
            (crashes - previous_crashes)::NUMERIC
            / previous_crashes * 100,
            2
        )

        ELSE NULL
    END AS crashes_yoy_pct

FROM yearly_data