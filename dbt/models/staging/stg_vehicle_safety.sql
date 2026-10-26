SELECT
    anio AS year,
    TRIM(vehiculo) AS vehicle_type,
    siniestros AS crashes,
    fallecidos AS fatalities,
    graves AS serious_injuries,
    menos_graves AS less_serious_injuries,
    leves AS minor_injuries,
    run_id

FROM {{ source('raw', 'vehicle_safety') }}